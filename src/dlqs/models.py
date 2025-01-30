"""Models for the DLQ Service"""

from typing import Any

from ghga_service_commons.utils.utc_dates import UTCDatetime, now_as_utc
from pydantic import BaseModel, Field


class EventInfo(BaseModel):
    """Model representing GHGA-internal event info"""

    topic: str = Field(
        ..., description="The name of the original topic the event was located in."
    )
    type_: str = Field(..., description="The 'type' given to the original event.")
    payload: dict[str, Any] = Field(..., description="The payload for the event.")
    key: str = Field(..., description="The key of the event.")
    timestamp: UTCDatetime = Field(
        default_factory=now_as_utc, description="The timestamp of the event."
    )
    headers: dict[str, str] = Field(
        ...,
        description="Any headers for the event. Must at least include correlation ID.",
    )


class StoredDLQEvent(EventInfo):
    """Model representing GHGA-internal event info with service name and event ID"""

    service: str = Field(
        ..., description="The name of the service that failed to process the event."
    )
    event_id: str = Field(
        ...,
        description=(
            "A comma-delimited concatenation of the service, topic, partition,"
            + " and offset that serves as the unique identifier for the event."
        ),
        examples=["ifrs,file-downloads,0,233"],
    )


class Services(BaseModel):
    """A model representing a service name and the topics it subscribes to."""

    service: str = Field(
        ...,
        description="The name of the service",
        examples=["ifrs", "dcs", "nos", "ns"],
    )
    topics: list[str] = Field(
        ...,
        description="A list of the names of the topics the service subscribes to.",
        examples=[["file-downloads", "users", "access-requests"]],
    )
