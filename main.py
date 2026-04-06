"""
Rev-Box Carrier Data Parser -- main entry point.

Usage:
    python main.py --file data/sample_carriers.xlsx
    python main.py --file data/sample_carriers.xlsx --export-csv
    python main.py --file data/sample_carriers.xlsx --db revbox_data.db --log-level DEBUG
"""
import argparse
import json
import logging
import sys
from pathlib import Path

from app.db import init_db, get_session
from app.extract import extract_sheets
from app.transform import transform_sheet
from app.load import load_records
from app.utils import setup_logging, export_policies_csv, print_summary


def parse_args():
    p = argparse.ArgumentParser(description="Rev-Box Carrier Data Parser")
    p.add_argument(
        "--file", required=True,
        help="Path to the carrier Excel workbook (e.g. data/sample_carriers.xlsx)"
    )
    p.add_argument(
        "--db", default="revbox_data.db",
        help="SQLite database file path (default: revbox_data.db)"
    )
    p.add_argument(
        "--export-csv", action="store_true",
        help="Export normalised policies to output/policies.csv after loading"
    )
    p.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO)"
    )
    return p.parse_args()


def run_pipeline(file_path: str, db_path: str, export_csv: bool) -> int:
    """
    Full ETL pipeline: extract -> transform -> load.
    Returns exit code (0 = success, 1 = error).
    """
    logger = logging.getLogger(__name__)

    # Init DB
    init_db(db_path)
    session = get_session(db_path)

    try:
        # EXTRACT
        sheets = extract_sheets(file_path)

        for sheet_name, df in sheets.items():
            logger.info(f"\n--- Processing sheet: {sheet_name} ---")

            # TRANSFORM
            # Use sheet name as carrier name (will be overridden by YAML if present)
            try:
                valid_records, raw_rows, col_mapping = transform_sheet(
                    df=df,
                    sheet_name=sheet_name,
                    carrier_name=sheet_name,
                )
            except Exception as e:
                logger.error(f"Transform failed for sheet '{sheet_name}': {e}")
                continue

            if not valid_records:
                logger.warning(f"No valid records from sheet '{sheet_name}', skipping load.")
                continue

            # LOAD
            try:
                run = load_records(
                    session=session,
                    records=valid_records,
                    raw_rows=raw_rows,
                    source_file=file_path,
                    sheet_name=sheet_name,
                )
            except Exception as e:
                logger.error(f"Load failed for sheet '{sheet_name}': {e}")
                session.rollback()
                continue

        # SUMMARY
        print_summary(session)

        # OPTIONAL CSV EXPORT
        if export_csv:
            count = export_policies_csv(session, output_path="output/policies.csv")
            logger.info(f"CSV export: {count} rows -> output/policies.csv")

        return 0

    except FileNotFoundError as e:
        logger.error(str(e))
        return 1
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        session.rollback()
        return 1
    finally:
        session.close()


def main():
    args = parse_args()
    setup_logging(args.log_level)
    sys.exit(run_pipeline(args.file, args.db, args.export_csv))


if __name__ == "__main__":
    main()
