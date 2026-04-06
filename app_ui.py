"""
Rev-Box Carrier Data Parser -- Streamlit UI

Run with: streamlit run app_ui.py
"""
import io
import tempfile

import pandas as pd
import streamlit as st

from app.db import init_db, get_session
from app.extract import extract_sheets
from app.transform import transform_sheet, load_carrier_config
from app.load import load_records
from app.ai_mapper import TARGET_SCHEMA

SCHEMA_OPTIONS = [f for f in TARGET_SCHEMA if f != "carrier_name"]  # carrier_name is injected automatically

st.set_page_config(
    page_title="Rev-Box Carrier Parser",
    page_icon="📋",
    layout="wide",
)

st.title("Rev-Box Carrier Data Parser")
st.caption("Upload a carrier Excel workbook to parse, map, validate, and load policy records.")

# ---- Sidebar ----
with st.sidebar:
    st.header("Settings")
    db_path = st.text_input("Database path", value="revbox_data.db")
    st.divider()
    st.markdown("**How it works**")
    st.markdown(
        "1. Upload an Excel file\n"
        "2. Review and adjust column mappings\n"
        "3. Run the pipeline\n"
        "4. Download the normalised CSV"
    )

# ---- File upload ----
uploaded_file = st.file_uploader(
    "Upload carrier workbook (.xlsx)",
    type=["xlsx"],
    help="Each sheet is treated as one carrier. Reference/lookup sheets are skipped automatically.",
)

if not uploaded_file:
    st.info("Upload an Excel file to get started.")
    st.stop()

# Save upload to temp file
with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
    tmp.write(uploaded_file.read())
    tmp_path = tmp.name

# ---- Extract sheets ----
try:
    sheets = extract_sheets(tmp_path)
except Exception as e:
    st.error(f"Failed to read workbook: {e}")
    st.stop()

st.success(f"Found {len(sheets)} carrier sheet(s): {', '.join(sheets.keys())}")

# ---- Column mapping (editable) ----
st.subheader("Column Mappings")
st.caption(
    "Mappings are pre-filled from YAML config. "
    "Override any field using the dropdowns before running the pipeline. "
    "Unknown columns are sent to Gemini AI if left unset."
)

# session_state key: "mappings" = { sheet_name: { col: schema_field } }
if "mappings" not in st.session_state:
    st.session_state.mappings = {}

user_mappings: dict[str, dict[str, str]] = {}

for sheet_name, df in sheets.items():
    config = load_carrier_config(sheet_name)
    yaml_col_map = config.get("column_mapping", {})

    with st.expander(f"{sheet_name}  --  {len(df)} rows, {len(df.columns)} columns", expanded=True):
        st.markdown(f"<small>Configure how <b>{sheet_name}</b> columns map to the database schema.</small>", unsafe_allow_html=True)

        sheet_mapping = {}
        cols_left, cols_right = st.columns(2)

        for i, col in enumerate(df.columns):
            yaml_val = yaml_col_map.get(col)
            # Determine default: use previous session state, then YAML, then first option
            prev_val = st.session_state.mappings.get(sheet_name, {}).get(col)
            default = prev_val or yaml_val or SCHEMA_OPTIONS[0]

            # Clamp to valid options
            if default not in SCHEMA_OPTIONS:
                default = SCHEMA_OPTIONS[0]

            source_label = "YAML" if yaml_val else "Manual"
            target_col = cols_left if i % 2 == 0 else cols_right

            with target_col:
                selected = st.selectbox(
                    label=f"{col}",
                    options=SCHEMA_OPTIONS,
                    index=SCHEMA_OPTIONS.index(default),
                    key=f"map_{sheet_name}_{col}",
                    help=f"Source: {source_label}" + (f" (suggested: {yaml_val})" if yaml_val else ""),
                )
                sheet_mapping[col] = selected

        user_mappings[sheet_name] = sheet_mapping

    # Persist selections in session state
    st.session_state.mappings[sheet_name] = sheet_mapping

# ---- Run pipeline ----
st.divider()
st.subheader("Run Pipeline")

col1, col2 = st.columns([1, 3])
with col1:
    run_btn = st.button("Run Pipeline", type="primary", use_container_width=True)
with col2:
    st.caption("Mappings above will be used as-is. No AI calls needed for pre-mapped columns.")

if run_btn:
    init_db(db_path)
    session = get_session(db_path)

    results = []
    progress = st.progress(0, text="Starting...")
    total = len(sheets)

    with st.spinner("Processing..."):
        for i, (sheet_name, df) in enumerate(sheets.items()):
            progress.progress(i / total, text=f"Processing {sheet_name}...")

            override = user_mappings.get(sheet_name)

            try:
                valid_records, raw_rows, col_mapping = transform_sheet(
                    df=df,
                    sheet_name=sheet_name,
                    carrier_name=sheet_name,
                    column_mapping_override=override,
                )
            except Exception as e:
                results.append({
                    "Sheet": sheet_name,
                    "Status": "Failed",
                    "Extracted": len(df),
                    "Loaded": 0,
                    "Duplicates": 0,
                    "Invalid": 0,
                    "Error": str(e),
                })
                continue

            invalid = len(df) - len(valid_records)

            if not valid_records:
                results.append({
                    "Sheet": sheet_name,
                    "Status": "No valid records",
                    "Extracted": len(df),
                    "Loaded": 0,
                    "Duplicates": 0,
                    "Invalid": invalid,
                    "Error": "",
                })
                continue

            try:
                run = load_records(
                    session=session,
                    records=valid_records,
                    raw_rows=raw_rows,
                    source_file=uploaded_file.name,
                    sheet_name=sheet_name,
                )
                results.append({
                    "Sheet": sheet_name,
                    "Status": "Success",
                    "Extracted": run.rows_extracted,
                    "Loaded": run.rows_loaded,
                    "Duplicates": run.rows_duplicate,
                    "Invalid": invalid,
                    "Error": "",
                })
            except Exception as e:
                session.rollback()
                results.append({
                    "Sheet": sheet_name,
                    "Status": "Load failed",
                    "Extracted": len(df),
                    "Loaded": 0,
                    "Duplicates": 0,
                    "Invalid": invalid,
                    "Error": str(e),
                })

        progress.progress(1.0, text="Done.")

    session.close()

    # ---- Results ----
    st.subheader("Results")
    results_df = pd.DataFrame(results)

    def colour_status(val):
        if val == "Success":
            return "color: green; font-weight: bold"
        if "fail" in str(val).lower():
            return "color: red; font-weight: bold"
        return "color: orange"

    st.dataframe(
        results_df.style.applymap(colour_status, subset=["Status"]),
        use_container_width=True,
        hide_index=True,
    )

    total_loaded = sum(r["Loaded"] for r in results)
    total_dupes = sum(r["Duplicates"] for r in results)
    total_invalid = sum(r["Invalid"] for r in results)

    m1, m2, m3 = st.columns(3)
    m1.metric("Policies Loaded", total_loaded)
    m2.metric("Duplicates Skipped", total_dupes)
    m3.metric("Invalid Rows", total_invalid)

    # ---- CSV download ----
    st.divider()
    st.subheader("Download")

    session2 = get_session(db_path)
    from app.models import Policy, Carrier
    policies = session2.query(Policy, Carrier.name).join(Carrier).all()
    session2.close()

    if policies:
        dl_rows = []
        for policy, carrier_name in policies:
            dl_rows.append({
                "policy_id": policy.policy_id,
                "carrier_name": carrier_name,
                "customer_name": policy.customer_name,
                "policy_type": policy.policy_type,
                "premium": policy.premium,
                "effective_date": policy.effective_date,
                "status": policy.status,
            })
        dl_df = pd.DataFrame(dl_rows)
        csv_bytes = dl_df.to_csv(index=False).encode("utf-8")

        st.download_button(
            label=f"Download policies.csv ({len(dl_rows)} rows)",
            data=csv_bytes,
            file_name="policies.csv",
            mime="text/csv",
            type="primary",
        )

        st.dataframe(dl_df.head(20), use_container_width=True, hide_index=True)
        if len(dl_rows) > 20:
            st.caption(f"Showing first 20 of {len(dl_rows)} rows.")
