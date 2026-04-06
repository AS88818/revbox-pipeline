"""
Extract stage: read Excel file, identify carrier sheets, return raw data.
"""
import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)

# Sheets to skip -- they contain reference/config data, not carrier records
REFERENCE_SHEET_NAMES = {
    "status_mappings", "policy_type_mappings", "policetype_mappings",
    "reference", "lookup", "config", "readme", "notes", "mappings"
}


def _is_reference_sheet(sheet_name: str) -> bool:
    """Return True if the sheet should be skipped (reference/config data)."""
    lower = sheet_name.lower().strip()
    if lower in REFERENCE_SHEET_NAMES:
        return True
    # Also skip sheets that contain "mapping" or "lookup" anywhere
    if "mapping" in lower or "lookup" in lower:
        return True
    return False


def extract_sheets(file_path: str) -> dict[str, pd.DataFrame]:
    """
    Read all carrier data sheets from an Excel workbook.

    Returns a dict of {sheet_name: DataFrame}, skipping reference sheets.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Excel file not found: {file_path}")

    logger.info(f"Opening workbook: {path.name}")
    xl = pd.ExcelFile(file_path)

    sheets = {}
    for sheet_name in xl.sheet_names:
        if _is_reference_sheet(sheet_name):
            logger.info(f"Skipping reference sheet: {sheet_name}")
            continue

        df = xl.parse(sheet_name, dtype=str)  # Read everything as string -- transform handles types
        df = df.dropna(how="all")             # Drop entirely empty rows
        df.columns = [str(c).strip() for c in df.columns]

        if df.empty:
            logger.warning(f"Sheet '{sheet_name}' is empty after cleaning, skipping.")
            continue

        sheets[sheet_name] = df
        logger.info(f"Extracted sheet '{sheet_name}': {len(df)} rows, {len(df.columns)} columns")

    if not sheets:
        raise ValueError(f"No usable carrier sheets found in: {file_path}")

    return sheets


def extract_reference_data(file_path: str) -> dict[str, pd.DataFrame]:
    """
    Read reference/lookup sheets if they exist.

    Returns a dict of {sheet_name: DataFrame} for known reference sheets.
    """
    xl = pd.ExcelFile(file_path)
    ref_data = {}

    for sheet_name in xl.sheet_names:
        if _is_reference_sheet(sheet_name):
            df = xl.parse(sheet_name, dtype=str).dropna(how="all")
            ref_data[sheet_name] = df
            logger.info(f"Loaded reference sheet: {sheet_name}")

    return ref_data
