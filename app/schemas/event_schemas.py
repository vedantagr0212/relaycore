from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator


class ChannelType(str, Enum):
    WEBHOOK = "WEBHOOK"
    SLACK = "SLACK"
    EMAIL = "EMAIL"


class EventStatus(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    DELIVERED = "DELIVERED"
    DEAD_LETTER = "DEAD_LETTER"


ALLOWED_STATUS_TRANSITIONS: Dict[EventStatus, set[EventStatus]] = {
    EventStatus.PENDING: {EventStatus.PROCESSING, EventStatus.DEAD_LETTER},
    EventStatus.PROCESSING: {
        EventStatus.DELIVERED,
        EventStatus.RETRY_SCHEDULED,
        EventStatus.DEAD_LETTER,
    },
    EventStatus.RETRY_SCHEDULED: {EventStatus.PROCESSING, EventStatus.DEAD_LETTER},
    EventStatus.DELIVERED: set(),
    EventStatus.DEAD_LETTER: {EventStatus.PENDING},
}


def validate_state_transition(current: EventStatus, target: EventStatus) -> None:
    if target not in ALLOWED_STATUS_TRANSITIONS.get(current, set()):
        raise ValueError(
            f"Illegal state transition from '{current.value}' to '{target.value}'."
        )


class EventIngestRequest(BaseModel):
    tenant_id: str = Field(..., min_length=3, max_length=64, pattern=r"^[a-zA-Z0-9_-]+$")
    idempotency_key: str = Field(..., min_length=8, max_length=128)
    event_type: str = Field(..., min_length=3, max_length=96)
    channel: ChannelType = Field(default=ChannelType.WEBHOOK)
    destination: str = Field(..., min_length=5, max_length=512)
    payload: Dict[str, Any] = Field(...)
    custom_headers: Optional[Dict[str, str]] = Field(default_factory=dict)
    max_retries: int = Field(default=5, ge=0, le=15)

    @field_validator("destination")
    @classmethod
    def validate_destination_by_channel(cls, v: str, info) -> str:
        channel = info.data.get("channel", ChannelType.WEBHOOK)
        if channel in (ChannelType.WEBHOOK, ChannelType.SLACK):
            if not (v.startswith("http://") or v.startswith("https://")):
                raise ValueError(f"{channel.value} destination must be a valid HTTP/HTTPS URL.")
        elif channel == ChannelType.EMAIL:
            if "@" not in v or "." not in v.split("@")[-1]:
                raise ValueError("EMAIL destination must be a valid email address.")
        return v

    @field_validator("payload")
    @classmethod
    def validate_non_empty_payload(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        if not v:
            raise ValueError("Event payload cannot be empty.")
        return v


class AttemptLogResponse(BaseModel):
    attempt_number: int
    status_code: Optional[int]
    latency_ms: float
    error_category: Optional[str]
    response_excerpt: Optional[str]
    attempted_at: datetime


class EventStatusResponse(BaseModel):
    event_id: str
    tenant_id: str
    idempotency_key: str
    event_type: str
    channel: ChannelType
    destination: str
    status: EventStatus
    retry_count: int
    max_retries: int
    next_retry_at: datetime
    last_error: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    is_idempotent_replay: bool = False
    attempts: List[AttemptLogResponse] = Field(default_factory=list)
