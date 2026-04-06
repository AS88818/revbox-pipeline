"""
SQLAlchemy ORM models for Rev-Box carrier data pipeline.
"""
from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Float, Date, DateTime,
    ForeignKey, UniqueConstraint, Text, Boolean
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class Carrier(Base):
    """Lookup table for insurance carriers."""
    __tablename__ = "carriers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    policies = relationship("Policy", back_populates="carrier")
    raw_records = relationship("RawRecord", back_populates="carrier")
    ingestion_runs = relationship("IngestionRun", back_populates="carrier")


class StatusLookup(Base):
    """Normalised policy status values."""
    __tablename__ = "ref_status"

    id = Column(Integer, primary_key=True, autoincrement=True)
    raw_value = Column(String(50), nullable=False, unique=True)
    normalised_value = Column(String(50), nullable=False)


class PolicyTypeLookup(Base):
    """Normalised policy type values."""
    __tablename__ = "ref_policy_type"

    id = Column(Integer, primary_key=True, autoincrement=True)
    raw_value = Column(String(50), nullable=False, unique=True)
    normalised_value = Column(String(50), nullable=False)


class Policy(Base):
    """Processed, normalised policy records."""
    __tablename__ = "policies"

    id = Column(Integer, primary_key=True, autoincrement=True)
    policy_id = Column(String(100), nullable=False)
    carrier_id = Column(Integer, ForeignKey("carriers.id"), nullable=False)
    customer_name = Column(String(200))
    policy_type = Column(String(50))
    premium = Column(Float)
    effective_date = Column(Date)
    status = Column(String(50))
    ingestion_run_id = Column(Integer, ForeignKey("ingestion_runs.id"))
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("policy_id", "carrier_id", name="uq_policy_carrier"),
    )

    carrier = relationship("Carrier", back_populates="policies")
    ingestion_run = relationship("IngestionRun", back_populates="policies")


class RawRecord(Base):
    """Raw ingestion log -- preserves source data before transformation."""
    __tablename__ = "raw_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    carrier_id = Column(Integer, ForeignKey("carriers.id"), nullable=False)
    ingestion_run_id = Column(Integer, ForeignKey("ingestion_runs.id"))
    sheet_name = Column(String(100))
    raw_data = Column(Text)  # JSON string of original row
    ingested_at = Column(DateTime, default=datetime.utcnow)

    carrier = relationship("Carrier", back_populates="raw_records")
    ingestion_run = relationship("IngestionRun", back_populates="raw_records")


class IngestionRun(Base):
    """One record per pipeline execution -- audit trail."""
    __tablename__ = "ingestion_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    carrier_id = Column(Integer, ForeignKey("carriers.id"), nullable=False)
    source_file = Column(String(500))
    sheet_name = Column(String(100))
    rows_extracted = Column(Integer, default=0)
    rows_loaded = Column(Integer, default=0)
    rows_skipped = Column(Integer, default=0)
    rows_duplicate = Column(Integer, default=0)
    status = Column(String(20), default="running")  # running | success | failed
    error_message = Column(Text)
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)

    carrier = relationship("Carrier", back_populates="ingestion_runs")
    policies = relationship("Policy", back_populates="ingestion_run")
    raw_records = relationship("RawRecord", back_populates="ingestion_run")
