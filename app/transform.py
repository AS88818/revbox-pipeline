"""
Transform stage: normalise raw DataFrames into validated PolicyRecord objects.
"""
import json
import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml
from pydantic import ValidationError

from app.schemas import PolicyRecord
from app.ai_mapper import map_unknown_columns

logger = logging.getLogger(__name__)

MAPPINGS_DIR = Path(__file__).parent.parent / "mappings"

# Date formats to try in order
DATE_FORMATS = [
    "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y",
    "%d-%m-%Y", "%m-%d-%Y", "%Y/%m/%d",
    "%d %b %Y", "%d %B %Y", "%b %d, %Y",
]


def load_carrier_config(carrier_name: str) -> dict:
    """
    Load YAML mapping config for a carrier.
    Falls back to empty config if no file exists (AI mapper will handle it).
    """
    safe_name = re.sub(r"[^\w\-]", "_", carrier_name.lower())
    yaml_path = MAPPINGS_DIR / f"{safe_name}.yml"

    if yaml_path.exists():
        with open(yaml_path, "r") as f:
            config = yaml.safe_load(f) or {}
        logger.info(f"Loaded config for carrier '{carrier_name}' from {yaml_path}")
    else:
        logger.warning(f"No YAML config for '{carrier_name}' -- AI mapper will classify all columns")
        config = {}

    config["_yaml_path"] = yaml_path
    return config


def normalise_date(value: str) -> Optional[date]:
    """Try multiple date formats, return None if all fail."""
    if not value or str(value).strip().lower() in ("nan", "none", ""):
        return None

    v = str(value).strip()

    # Handle Excel serial date numbers
    if re.match(r"^\d{5}$", v):
        try:
            return (datetime(1899, 12, 30) + pd.Timedelta(days=int(v))).date()
        except Exception:
            pass

    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(v, fmt).date()
        except ValueError:
            continue

    logger.warning(f"Could not parse date value: '{v}'")
    return None


def normalise_currency(value: str) -> Optional[float]:
    """Strip currency symbols/commas, return float or None."""
    if not value or str(value).strip().lower() in ("nan", "none", ""):
        return None

    v = str(value).strip()
    # Remove currency symbols, commas, spaces
    v = re.sub(r"[^\d.\-]", "", v)

    try:
        return float(v)
    except ValueError:
        logger.warning(f"Could not parse currency value: '{value}'")
        return None


def get_column_mapping(
    df: pd.DataFrame,
    config: dict,
    carrier_name: str,
) -> dict[str, str]:
    """
    Build full column -> schema_field mapping using YAML config + AI fallback.
    """
    yaml_mapping = config.get("column_mapping", {})
    all_columns = list(df.columns)
    sample_rows = df.head(10).to_dict(orient="records")
    yaml_path = config.get("_yaml_path")

    return map_unknown_columns(
        columns=all_columns,
        existing_mapping=yaml_mapping,
        sample_rows=sample_rows,
        yaml_path=yaml_path,
    )


def transform_sheet(
    df: pd.DataFrame,
    sheet_name: str,
    carrier_name: str,
    column_mapping_override: Optional[dict[str, str]] = None,
) -> tuple[list[PolicyRecord], list[dict], dict[str, str]]:
    """
    Transform a raw DataFrame into validated PolicyRecord objects.

    Args:
        column_mapping_override: If provided, skips YAML/AI lookup and uses this mapping directly.
                                 Useful for UI-driven manual overrides.

    Returns:
        (valid_records, raw_rows_json, column_mapping)
    """
    if column_mapping_override is not None:
        col_mapping = column_mapping_override
        logger.info(f"Using user-provided column mapping override for '{carrier_name}' / '{sheet_name}'")
    else:
        config = load_carrier_config(carrier_name)
        col_mapping = get_column_mapping(df, config, carrier_name)

    logger.info(f"Column mapping for '{carrier_name}' / '{sheet_name}': {col_mapping}")

    valid_records: list[PolicyRecord] = []
    raw_rows_json: list[dict] = []

    for idx, row in df.iterrows():
        raw_row = row.to_dict()
        raw_rows_json.append(raw_row)

        # Remap columns to schema fields
        mapped: dict = {}
        for raw_col, schema_field in col_mapping.items():
            if schema_field == "ignore_column":
                continue
            if raw_col in raw_row:
                mapped[schema_field] = raw_row[raw_col]

        # Apply type coercions before Pydantic validation
        if "premium" in mapped:
            mapped["premium"] = normalise_currency(str(mapped["premium"]))

        if "effective_date" in mapped:
            mapped["effective_date"] = normalise_date(str(mapped["effective_date"]))

        # Inject carrier name if not in the data
        if "carrier_name" not in mapped or not mapped["carrier_name"]:
            mapped["carrier_name"] = carrier_name

        # Skip rows missing the primary key
        if not mapped.get("policy_id") or str(mapped["policy_id"]).strip().lower() in ("nan", "none", ""):
            logger.debug(f"Row {idx}: missing policy_id, skipping")
            continue

        try:
            record = PolicyRecord(**mapped)
            valid_records.append(record)
        except ValidationError as e:
            logger.warning(f"Row {idx} failed validation: {e.errors()} | raw: {mapped}")

    logger.info(
        f"Transform complete for '{carrier_name}' / '{sheet_name}': "
        f"{len(valid_records)} valid, {len(raw_rows_json) - len(valid_records)} invalid"
    )

    return valid_records, raw_rows_json, col_mapping
