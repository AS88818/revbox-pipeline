# Rev-Box Carrier Data Parser

A pipeline that takes messy carrier Excel data, figures out what each column means, cleans it, and loads it into a structured SQLite database. Built to handle the reality that every carrier sends data differently.

Adding a new carrier takes one YAML file. No code changes. The system is designed to scale to 50+ carriers without touching the pipeline logic.

---

## How it works

```
Excel Workbook
     |
 [EXTRACT]   reads carrier sheets, skips reference/lookup tabs
     |
 [TRANSFORM] maps columns, normalises dates/currency, validates each row
     |
  [LOAD]     deduplicates and inserts to SQLite, logs every run
     |
  SQLite DB  +  optional CSV export
```

The column mapping step is the interesting part. Each carrier gets a YAML config file that says "their column X = our field Y". If a column shows up that isn't in the config, Gemini classifies it and saves the result back to the YAML so you don't pay for the API call twice.

---

## Setup

```bash
pip install -r requirements.txt
```

Create a `.env` file with your Gemini API key:

```
GOOGLE_API_KEY=your_key_here
```

The AI mapper only runs when it hits a column that isn't already in the YAML config. For the included sample data, all columns are pre-mapped so no API calls are made on first run.

---

## Usage

### Web UI

```bash
streamlit run app_ui.py
```

Opens at `http://localhost:8501`. Two tabs:

**Run Pipeline** -- Upload a workbook, review how every column is going to be mapped (YAML-sourced mappings show green, AI-pending show orange), override anything that looks wrong, run the pipeline, download the CSV.

**New Carrier Setup** -- Got a carrier that isn't in the system yet? Upload a sample file (or type the column names manually), assign each column to a schema field using dropdowns, preview the generated YAML, and save it. No code changes, no file editing. The next run picks it up automatically.

### CLI

```bash
python main.py --file data/sample_carriers.xlsx

# With CSV export
python main.py --file data/sample_carriers.xlsx --export-csv

# Verbose mode
python main.py --file data/sample_carriers.xlsx --log-level DEBUG
```

### Sample data

```bash
python generate_sample_data.py
```

Generates `data/sample_carriers.xlsx` with 3 messy carrier sheets (different column names, date formats, currency formats, duplicates, nulls, one bad date row).

---

## Adding a new carrier

**Option 1 -- UI (recommended):** Open the *New Carrier Setup* tab in the Streamlit app. Upload a sample file or enter the column headers manually, assign each one to a schema field, and click Save. Done.

**Option 2 -- Manual:** Create `mappings/<carrier_name>.yml` directly:

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

Either way, drop the file in your workbook and run. Any columns not covered by the YAML go to Gemini, and the result gets written back automatically so the next run doesn't call the API again.

---

## Project structure

```
Rev-Box Project/
├── app/
│   ├── models.py       # SQLAlchemy ORM: Carrier, Policy, RawRecord, IngestionRun
│   ├── schemas.py      # Pydantic validation: PolicyRecord
│   ├── db.py           # Engine and session management
│   ├── extract.py      # Read Excel, skip reference sheets
│   ├── transform.py    # Column mapping, type coercion, row validation
│   ├── ai_mapper.py    # Gemini column classifier
│   ├── load.py         # DB insert, deduplication, ingestion run logging
│   └── utils.py        # Logging setup, CSV export, summary output
├── mappings/
│   ├── carrier_alpha.yml
│   ├── carrier_beta.yml
│   └── carrier_gamma.yml
├── data/
│   └── sample_carriers.xlsx
├── output/             # CSV exports go here
├── app_ui.py           # Streamlit UI
├── main.py             # CLI entry point
├── generate_sample_data.py
└── requirements.txt
```

---

## Database schema

| Table | Purpose |
|-------|---------|
| `carriers` | Carrier lookup |
| `policies` | Cleaned policy records (unique on `policy_id + carrier_id`) |
| `raw_records` | Source row audit log before transformation |
| `ingestion_runs` | Per-run stats: rows extracted, loaded, skipped, duplicated |
| `ref_status` | Status normalisation lookup |
| `ref_policy_type` | Policy type normalisation lookup |

---

## Assumptions

- Each Excel sheet = one carrier. Sheet name is used as the carrier identifier unless the YAML config specifies otherwise.
- The pipeline doesn't auto-detect which sheet is a reference sheet by content -- it goes by name. Sheets named `Status_Mappings`, `Lookup`, etc. are skipped. Anything else is treated as carrier data.
- Deduplication is on `policy_id + carrier_id`. Same policy from two different carriers is allowed. Same policy from the same carrier twice is a duplicate.
- Currency values can come in with symbols (`$`, `USD`) or commas, all stripped before parsing. If it still can't parse, the premium is set to null and the row is kept.
- Dates: 9 formats tried in order. If none match, effective_date is null and the row is still loaded. A warning is logged.
- A row missing `policy_id` is dropped entirely. That's the only hard discard.

---

## Tradeoffs and limitations

**SQLite over Postgres**: SQLite is zero-setup and fits a test project. Swapping to Postgres is one line change in `db.py`. The ORM handles the rest.

**AI mapper caches to YAML**: Once Gemini classifies a column, it writes the result back to the carrier YAML. That means after first run, no more API calls for that carrier. The downside is if Gemini gets it wrong, it stays wrong until someone edits the YAML. The UI override handles this.

**No schema migration**: The DB schema is created once on first run. If you need to change the schema, delete the `.db` file and rebuild. Not a problem for a v1, but worth noting for production.

**Built without the real data file**: The carrier Excel file wasn't provided, so I generated synthetic data that replicates the described messiness: different column names per carrier, mixed date and currency formats, duplicates, nulls, and one unparseable date. Every edge case in the brief is covered in the sample. When the real file arrives, drop it in `/data` and run, no changes needed.
