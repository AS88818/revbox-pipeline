"""
Generate synthetic sample_carriers.xlsx with 3 messy carrier sheets + reference sheets.
Run once: python generate_sample_data.py
"""
import random
from pathlib import Path
import pandas as pd

random.seed(42)

OUTPUT = Path("data/sample_carriers.xlsx")
OUTPUT.parent.mkdir(exist_ok=True)

FIRST_NAMES = ["James", "Sarah", "David", "Emma", "Michael", "Olivia", "Robert", "Sophia",
               "William", "Isabella", "Ahmed", "Fatima", "Lim", "Tan", "Siti", "Raj"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
              "Wilson", "Moore", "Abdullah", "Hassan", "Wong", "Chong", "Rahman", "Kumar"]

POLICY_TYPES_A = ["Auto", "Home", "Life", "Health", "Commercial", "Liability"]
POLICY_TYPES_B = ["AUT", "HOM", "LIF", "HLT", "COM", "LIA"]
POLICY_TYPES_G = ["Automobile Coverage", "Homeowners", "Life Insurance", "Health Plan", "Commercial Lines", "Liability"]

STATUSES_A = ["Active", "Inactive", "Cancelled", "Pending"]
STATUSES_B = ["ACT", "INA", "CAN", "PND", "LAP"]
STATUSES_G = ["ACTIVE", "INACTIVE", "CANCELLED", "PENDING", "LAPSED"]


def random_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def random_date_str(fmt):
    year = random.choice([2022, 2023, 2024, 2025])
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    from datetime import date
    d = date(year, month, day)
    return d.strftime(fmt)


def random_premium():
    base = random.choice([500, 750, 1200, 1800, 2400, 3600, 5000])
    noise = random.randint(-50, 50)
    return base + noise


# ---- Carrier Alpha -- clean-ish, dates as DD/MM/YYYY, $ in premium ----
def make_alpha(n=30):
    rows = []
    for i in range(1, n + 1):
        # Introduce a duplicate every 10 rows
        pol_id = f"ALPHA-{i:04d}" if i % 10 != 0 else f"ALPHA-{(i-1):04d}"
        rows.append({
            "Policy Number": pol_id,
            "Insured Name": random_name(),
            "Type of Policy": random.choice(POLICY_TYPES_A),
            "Annual Premium": f"${random_premium():,.2f}",
            "Start Date": random_date_str("%d/%m/%Y"),
            "Policy Status": random.choice(STATUSES_A),
            "Agent Code": f"AGT{random.randint(100, 999)}",
            "Branch": random.choice(["KL", "Penang", "JB", "Ipoh"]),
        })
    # Introduce a few nulls
    rows[5]["Insured Name"] = None
    rows[12]["Annual Premium"] = None
    rows[20]["Start Date"] = None
    return pd.DataFrame(rows)


# ---- Carrier Beta -- cryptic headers, dates as YYYY-MM-DD, plain number premium ----
def make_beta(n=25):
    rows = []
    for i in range(1, n + 1):
        pol_id = f"B-{random.randint(10000, 99999)}"
        rows.append({
            "POL_ID": pol_id,
            "CLIENT": random_name(),
            "POL_TYPE": random.choice(POLICY_TYPES_B),
            "PREM_USD": random_premium(),
            "EFF_DT": random_date_str("%Y-%m-%d"),
            "STAT_CD": random.choice(STATUSES_B),
            "REGION_CODE": random.choice(["MY", "SG", "ID", "TH"]),
            "UNDERWRITER_ID": f"UW{random.randint(1, 20):02d}",
            "NOTES": random.choice(["", "Renewal", "New business", "Mid-term adjustment", ""]),
        })
    # Duplicate row
    rows.append(rows[3].copy())
    # Null policy_id row (should be skipped)
    rows.append({
        "POL_ID": None,
        "CLIENT": "Ghost Record",
        "POL_TYPE": "AUT",
        "PREM_USD": 999,
        "EFF_DT": "2024-01-01",
        "STAT_CD": "ACT",
        "REGION_CODE": "MY",
        "UNDERWRITER_ID": "UW01",
        "NOTES": "",
    })
    return pd.DataFrame(rows)


# ---- Carrier Gamma -- verbose names, dates as "Jan 15, 2024", premium with currency symbol ----
def make_gamma(n=20):
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "Reference ID": f"GMA-REF-{random.randint(1000, 9999)}",
            "Full Name of Insured": random_name(),
            "Coverage Category": random.choice(POLICY_TYPES_G),
            "Total Premium Amount ($)": f"USD {random_premium():,.2f}",
            "Coverage Effective Date": random_date_str("%d %b %Y"),
            "Current Status": random.choice(STATUSES_G),
            "Internal Ref": f"INT-{random.randint(100, 999)}",
            "Last Modified By": random.choice(["admin", "ops_user", "api_sync"]),
        })
    # A row with a junk date
    rows[7]["Coverage Effective Date"] = "NOT A DATE"
    return pd.DataFrame(rows)


# ---- Reference sheets ----
status_mappings = pd.DataFrame([
    {"raw_value": "Active", "normalised_value": "active"},
    {"raw_value": "Inactive", "normalised_value": "inactive"},
    {"raw_value": "Cancelled", "normalised_value": "cancelled"},
    {"raw_value": "Pending", "normalised_value": "pending"},
    {"raw_value": "ACT", "normalised_value": "active"},
    {"raw_value": "INA", "normalised_value": "inactive"},
    {"raw_value": "CAN", "normalised_value": "cancelled"},
    {"raw_value": "PND", "normalised_value": "pending"},
    {"raw_value": "LAP", "normalised_value": "lapsed"},
    {"raw_value": "ACTIVE", "normalised_value": "active"},
    {"raw_value": "INACTIVE", "normalised_value": "inactive"},
    {"raw_value": "CANCELLED", "normalised_value": "cancelled"},
    {"raw_value": "LAPSED", "normalised_value": "lapsed"},
])

policy_type_mappings = pd.DataFrame([
    {"raw_value": "Auto", "normalised_value": "auto"},
    {"raw_value": "AUT", "normalised_value": "auto"},
    {"raw_value": "Automobile Coverage", "normalised_value": "auto"},
    {"raw_value": "Home", "normalised_value": "home"},
    {"raw_value": "HOM", "normalised_value": "home"},
    {"raw_value": "Homeowners", "normalised_value": "home"},
    {"raw_value": "Life", "normalised_value": "life"},
    {"raw_value": "LIF", "normalised_value": "life"},
    {"raw_value": "Life Insurance", "normalised_value": "life"},
    {"raw_value": "Health", "normalised_value": "health"},
    {"raw_value": "HLT", "normalised_value": "health"},
    {"raw_value": "Health Plan", "normalised_value": "health"},
    {"raw_value": "Commercial", "normalised_value": "commercial"},
    {"raw_value": "COM", "normalised_value": "commercial"},
    {"raw_value": "Commercial Lines", "normalised_value": "commercial"},
    {"raw_value": "Liability", "normalised_value": "liability"},
    {"raw_value": "LIA", "normalised_value": "liability"},
])


with pd.ExcelWriter(OUTPUT, engine="openpyxl") as writer:
    make_alpha().to_excel(writer, sheet_name="Carrier Alpha", index=False)
    make_beta().to_excel(writer, sheet_name="Carrier Beta", index=False)
    make_gamma().to_excel(writer, sheet_name="Carrier Gamma", index=False)
    status_mappings.to_excel(writer, sheet_name="Status_Mappings", index=False)
    policy_type_mappings.to_excel(writer, sheet_name="PolicyType_Mappings", index=False)

print(f"Sample data generated: {OUTPUT}")
