"""
AI-assisted column mapping using Google Gemini.

When a carrier sheet has columns not covered by the static YAML config,
this module sends them to Gemini for classification against our schema.
Results are cached back into the YAML so future runs don't re-call the API.
"""
import json
import logging
import os
from pathlib import Path

from google import genai
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

TARGET_SCHEMA = [
    "policy_id",
    "customer_name",
    "policy_type",
    "premium",
    "effective_date",
    "status",
    "carrier_name",
    "ignore_column",
]

_client = None


def _get_client():
    global _client
    if _client is None:
        api_key = os.getenv("GOOGLE_API_KEY")
        if not api_key:
            raise EnvironmentError(
                "GOOGLE_API_KEY not found in environment. Add it to your .env file."
            )
        _client = genai.Client(api_key=api_key)
    return _client


def map_single_column(unknown_header: str, sample_values: list) -> str:
    """
    Ask Gemini to map one unknown column header to a schema field.

    Returns one of TARGET_SCHEMA values. Falls back to 'ignore_column' on any error.
    """
    client = _get_client()

    prompt = f"""You are an expert data engineer mapping messy insurance carrier data to a strict database schema.

Unknown Column Header: '{unknown_header}'
Sample Values from this column: {sample_values[:5]}

Map this column to EXACTLY ONE of the following valid schema fields:
{", ".join(TARGET_SCHEMA)}

Rules:
- If the column is irrelevant, junk data, or an internal carrier field with no schema equivalent, output 'ignore_column'
- Return ONLY the exact string of the matched field
- No punctuation, no explanation, no quotes
"""

    try:
        logger.info(f"Gemini analysing unknown column: '{unknown_header}'...")
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt,
        )
        result = response.text.strip().lower()

        if result not in TARGET_SCHEMA:
            logger.warning(
                f"Gemini returned invalid schema field '{result}' for column '{unknown_header}' -- defaulting to ignore_column"
            )
            return "ignore_column"

        logger.info(f"Gemini mapped '{unknown_header}' -> '{result}'")
        return result

    except Exception as e:
        logger.error(f"Gemini mapping failed for '{unknown_header}': {e}")
        return "ignore_column"


def map_unknown_columns(
    columns: list[str],
    existing_mapping: dict[str, str],
    sample_rows: list[dict],
    yaml_path: Path | None = None,
) -> dict[str, str]:
    """
    For every column not already in existing_mapping, ask Gemini.
    Optionally saves new mappings back to yaml_path so future runs skip the API.

    Returns the complete mapping dict (existing + new).
    """
    mapping = dict(existing_mapping)
    new_mappings = {}

    for col in columns:
        if col in mapping:
            continue  # Already mapped by YAML config

        # Collect sample values for this column
        samples = [str(row.get(col, "")) for row in sample_rows[:10] if row.get(col)]
        result = map_single_column(col, samples)
        mapping[col] = result
        new_mappings[col] = result

    if new_mappings and yaml_path:
        _append_to_yaml(yaml_path, new_mappings)

    return mapping


def _append_to_yaml(yaml_path: Path, new_mappings: dict[str, str]) -> None:
    """Append newly discovered mappings to the carrier YAML config."""
    import yaml

    if yaml_path.exists():
        with open(yaml_path, "r") as f:
            config = yaml.safe_load(f) or {}
    else:
        config = {}

    col_map = config.get("column_mapping", {})
    for raw_col, schema_field in new_mappings.items():
        if schema_field != "ignore_column":
            col_map[raw_col] = schema_field

    config["column_mapping"] = col_map

    with open(yaml_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, allow_unicode=True)

    logger.info(f"Saved {len(new_mappings)} new AI mappings to {yaml_path}")
