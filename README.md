# Rev-Box Carrier Data Parser

ETL pipeline that ingests insurance carrier data from Excel workbooks, normalises messy column names via AI-assisted mapping, validates records with Pydantic, and loads them into a structured SQLite database.

---

## Architecture

```
Excel Workbook
     |
 [EXTRACT]  -- reads all carrier sheets, skips reference/lookup tabs
     |
 [TRANSFORM] -- maps columns (YAML config + Gemini fallback), coerces types, validates
     |
  [LOAD]    -- deduplicates, inserts to SQLite, logs every run
     |
  SQLite DB  +  CSV Export (optional)
```

### Key design decisions

| Decision | Rationale |
|----------|-----------|
| YAML-first column mapping | Zero API calls for known carriers; Gemini only fires on unmapped columns |
| Pydantic validation layer | Catches bad data before it touches the DB; validation errors are logged not crashed |
| Raw record audit table | Every source row is preserved before transformation |
| IngestionRun table | Full per-run audit trail: rows extracted, loaded, duped, skipped |
| Deduplication on `policy_id + carrier_id` | Idempotent -- re-running the same file does not create duplicates |

---

## Setup

### 1. Clone and install dependencies

```bash
pip install -r requirements.txt
```

### 2. Set your Gemini API key

Create a `.env` file in the project root:

```
GOOGLE_API_KEY=your_key_here
```

The AI mapper only calls Gemini when a carrier sheet has columns not already covered by its YAML config. For the included sample data, all columns are pre-mapped so no API calls are made.

---

## Usage

### Option A: Web UI (recommended for demos)

```bash
streamlit run app_ui.py
```

Opens a browser interface at `http://localhost:8501`. Upload the Excel file, preview column mappings, run the pipeline, and download the output CSV -- all in one page.

### Option B: CLI

```bash
# Basic run
python main.py --file data/sample_carriers.xlsx

# With CSV export
python main.py --file data/sample_carriers.xlsx --export-csv

# Custom DB path and verbose logging
python main.py --file data/sample_carriers.xlsx --db custom.db --log-level DEBUG
```

### Generate sample data (first time)

```bash
python generate_sample_data.py
```

This creates `data/sample_carriers.xlsx` with 3 carrier sheets (30 + 27 + 20 rows) including intentional messiness: duplicate policy IDs, null values, mixed date formats, currency symbols, cryptic column names, and one unparseable date.

---

## Project structure

```
Rev-Box Project/
├── app/
│   ├── models.py       # SQLAlchemy ORM: Carrier, Policy, RawRecord, IngestionRun
│   ├── schemas.py      # Pydantic validation: PolicyRecord
│   ├── db.py           # Engine/session management
│   ├── extract.py      # Read Excel, skip reference sheets
│   ├── transform.py    # Column mapping, type coercion, validation
│   ├── ai_mapper.py    # Gemini-powered column classification
│   ├── load.py         # DB insert, deduplication, run logging
│   └── utils.py        # Logging setup, CSV export, summary printer
├── mappings/
│   ├── carrier_alpha.yml
│   ├── carrier_beta.yml
│   └── carrier_gamma.yml
├── data/
│   └── sample_carriers.xlsx
├── output/             # CSV exports land here
├── main.py             # CLI entry point
├── generate_sample_data.py
├── requirements.txt
└── Dockerfile
```

---

## Adding a new carrier

1. Create `mappings/<carrier_name>.yml` (lowercase, spaces as underscores):

```yaml
carrier_name: My New Carrier

column_mapping:
  Their Column Header: policy_id
  Customer Full Name: customer_name
  Coverage Type: policy_type
  Premium Amount: premium
  Policy Start: effective_date
  Status Flag: status
  Internal ID: ignore_column
```

2. Run the pipeline. Any unmapped columns will be sent to Gemini and the result appended to the YAML automatically.

---

## Database schema

| Table | Purpose |
|-------|---------|
| `carriers` | Carrier lookup |
| `policies` | Normalised policy records (unique on `policy_id + carrier_id`) |
| `raw_records` | Source row audit log (JSON) |
| `ingestion_runs` | Per-run stats and status |
| `ref_status` | Status normalisation lookup |
| `ref_policy_type` | Policy type normalisation lookup |

---

## Containerisation

A Dockerfile is not included. `requirements.txt` and Python 3.11+ are all you need to run this locally. If containerisation is needed for deployment, a basic Dockerfile can be added in under 10 lines.
