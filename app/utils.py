"""
Shared utilities: logging setup, CSV export, summary reporting.
"""
import csv
import logging
import sys
from pathlib import Path

from sqlalchemy.orm import Session

from app.models import Policy, Carrier, IngestionRun


def setup_logging(level: str = "INFO") -> None:
    """Configure root logger with clean format."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def export_policies_csv(session: Session, output_path: str = "output/policies.csv") -> int:
    """
    Export all normalised policies to a CSV file.
    Returns number of rows written.
    """
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    policies = (
        session.query(Policy, Carrier.name)
        .join(Carrier)
        .all()
    )

    if not policies:
        logging.getLogger(__name__).warning("No policies found to export.")
        return 0

    fieldnames = [
        "id", "policy_id", "carrier_name", "customer_name",
        "policy_type", "premium", "effective_date", "status",
        "ingestion_run_id", "created_at",
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for policy, carrier_name in policies:
            writer.writerow({
                "id": policy.id,
                "policy_id": policy.policy_id,
                "carrier_name": carrier_name,
                "customer_name": policy.customer_name,
                "policy_type": policy.policy_type,
                "premium": policy.premium,
                "effective_date": policy.effective_date,
                "status": policy.status,
                "ingestion_run_id": policy.ingestion_run_id,
                "created_at": policy.created_at,
            })

    logging.getLogger(__name__).info(f"Exported {len(policies)} policies to {path}")
    return len(policies)


def print_summary(session: Session) -> None:
    """Print a human-readable run summary to stdout."""
    runs = session.query(IngestionRun).order_by(IngestionRun.id.desc()).limit(20).all()

    if not runs:
        print("No ingestion runs found.")
        return

    print("\n" + "=" * 60)
    print("INGESTION SUMMARY")
    print("=" * 60)

    total_loaded = 0
    total_dupes = 0

    for run in reversed(runs):
        carrier = session.query(Carrier).get(run.carrier_id)
        carrier_name = carrier.name if carrier else "Unknown"
        duration = ""
        if run.completed_at and run.started_at:
            secs = (run.completed_at - run.started_at).total_seconds()
            duration = f" ({secs:.1f}s)"

        print(
            f"  [{run.status.upper()}] {carrier_name} / {run.sheet_name}{duration}"
            f"\n    extracted={run.rows_extracted}  loaded={run.rows_loaded}"
            f"  duplicates={run.rows_duplicate}  skipped={run.rows_skipped}"
        )
        total_loaded += run.rows_loaded or 0
        total_dupes += run.rows_duplicate or 0

    total_policies = session.query(Policy).count()
    print("=" * 60)
    print(f"  Total policies in DB : {total_policies}")
    print(f"  Loaded this run      : {total_loaded}")
    print(f"  Duplicates skipped   : {total_dupes}")
    print("=" * 60 + "\n")
