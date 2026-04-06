"""
Load stage: persist validated records to SQLite, log ingestion runs.
"""
import json
import logging
from datetime import datetime
from typing import Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import Carrier, Policy, RawRecord, IngestionRun
from app.schemas import PolicyRecord

logger = logging.getLogger(__name__)


def get_or_create_carrier(session: Session, carrier_name: str) -> Carrier:
    """Return existing carrier row or create a new one."""
    carrier = session.query(Carrier).filter_by(name=carrier_name).first()
    if not carrier:
        carrier = Carrier(name=carrier_name)
        session.add(carrier)
        session.flush()
        logger.info(f"Created new carrier: '{carrier_name}' (id={carrier.id})")
    return carrier


def create_ingestion_run(
    session: Session,
    carrier: Carrier,
    source_file: str,
    sheet_name: str,
) -> IngestionRun:
    run = IngestionRun(
        carrier_id=carrier.id,
        source_file=source_file,
        sheet_name=sheet_name,
        status="running",
    )
    session.add(run)
    session.flush()
    return run


def load_raw_records(
    session: Session,
    carrier: Carrier,
    ingestion_run: IngestionRun,
    sheet_name: str,
    raw_rows: list[dict],
) -> None:
    """Persist raw source rows to the audit table."""
    for row in raw_rows:
        raw = RawRecord(
            carrier_id=carrier.id,
            ingestion_run_id=ingestion_run.id,
            sheet_name=sheet_name,
            raw_data=json.dumps(row, default=str),
        )
        session.add(raw)


def load_records(
    session: Session,
    records: list[PolicyRecord],
    raw_rows: list[dict],
    source_file: str,
    sheet_name: str,
) -> IngestionRun:
    """
    Load a batch of validated PolicyRecord objects into the database.

    Returns the completed IngestionRun for reporting.
    """
    if not records:
        logger.warning(f"No valid records to load for sheet '{sheet_name}'")

    # Infer carrier name from the first record (all records in a sheet share it)
    carrier_name = records[0].carrier_name if records else sheet_name

    carrier = get_or_create_carrier(session, carrier_name)
    run = create_ingestion_run(session, carrier, source_file, sheet_name)

    # Persist raw rows for audit trail
    load_raw_records(session, carrier, run, sheet_name, raw_rows)

    loaded = 0
    skipped = 0
    duplicates = 0

    for record in records:
        # Check for existing policy (deduplication on policy_id + carrier_id)
        existing = (
            session.query(Policy)
            .filter_by(policy_id=record.policy_id, carrier_id=carrier.id)
            .first()
        )

        if existing:
            logger.debug(f"Duplicate skipped: policy_id='{record.policy_id}', carrier='{carrier_name}'")
            duplicates += 1
            continue

        policy = Policy(
            policy_id=record.policy_id,
            carrier_id=carrier.id,
            customer_name=record.customer_name,
            policy_type=record.policy_type,
            premium=record.premium,
            effective_date=record.effective_date,
            status=record.status,
            ingestion_run_id=run.id,
        )

        try:
            session.add(policy)
            session.flush()
            loaded += 1
        except IntegrityError:
            session.rollback()
            logger.warning(f"IntegrityError on policy_id='{record.policy_id}', skipping")
            duplicates += 1

    # Finalise the run stats
    run.rows_extracted = len(raw_rows)
    run.rows_loaded = loaded
    run.rows_skipped = skipped
    run.rows_duplicate = duplicates
    run.status = "success"
    run.completed_at = datetime.utcnow()

    session.commit()

    logger.info(
        f"Load complete -- carrier='{carrier_name}', sheet='{sheet_name}': "
        f"loaded={loaded}, duplicates={duplicates}, skipped={skipped}"
    )

    return run
