"""
Pydantic schemas for validation before DB load.
"""
from datetime import date
from typing import Optional
from pydantic import BaseModel, field_validator, model_validator
import re


VALID_STATUSES = {"active", "inactive", "cancelled", "pending", "lapsed"}
VALID_POLICY_TYPES = {"auto", "home", "life", "health", "commercial", "liability", "other"}


class PolicyRecord(BaseModel):
    """Validated, normalised policy record ready for DB insert."""

    policy_id: str
    carrier_name: str
    customer_name: Optional[str] = None
    policy_type: Optional[str] = None
    premium: Optional[float] = None
    effective_date: Optional[date] = None
    status: Optional[str] = None

    @field_validator("policy_id")
    @classmethod
    def policy_id_not_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("policy_id cannot be empty")
        return v

    @field_validator("customer_name")
    @classmethod
    def clean_customer_name(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = v.strip()
        return v if v else None

    @field_validator("premium")
    @classmethod
    def premium_non_negative(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and v < 0:
            raise ValueError(f"premium cannot be negative: {v}")
        return v

    @field_validator("status")
    @classmethod
    def normalise_status(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        return v.strip().lower()

    @field_validator("policy_type")
    @classmethod
    def normalise_policy_type(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        return v.strip().lower()

    model_config = {"str_strip_whitespace": True}


class CarrierSheetData(BaseModel):
    """Parsed sheet metadata passed between pipeline stages."""
    carrier_name: str
    sheet_name: str
    records: list[dict]
    column_mapping: dict[str, str]
