from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field, field_serializer


EventType = Literal["page_enter", "page_leave", "route_change", "click", "mouse_move"]


class ClickstreamEvent(BaseModel):
    event_id: str = Field(min_length=1, max_length=120)
    session_id: str = Field(min_length=1, max_length=120)
    user_id: UUID | None = None
    event_type: EventType
    page: str = Field(min_length=1, max_length=240)
    ts: datetime
    element_id: str | None = Field(default=None, max_length=160)
    x: int | None = None
    y: int | None = None
    dwell_ms: int | None = Field(default=None, ge=0)
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_serializer("ts")
    def serialize_ts(self, value: datetime) -> str:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{value.microsecond // 1000:03d}Z"


class ClickstreamBatch(BaseModel):
    events: list[ClickstreamEvent] = Field(min_length=1, max_length=200)


class PublishResponse(BaseModel):
    accepted: int
    topic: str

