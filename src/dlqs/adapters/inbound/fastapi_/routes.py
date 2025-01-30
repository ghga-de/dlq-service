"""FastAPI endpoint function definitions"""

from fastapi import APIRouter, status

from dlqs.adapters.inbound.fastapi_.dummies import (
    DLQConfigDependency,
    DLQManagerDependency,
)
from dlqs.models import EventInfo

router = APIRouter()


@router.get(
    "/health",
    summary="health",
    status_code=status.HTTP_200_OK,
)
async def health():
    """Used to test if this service is alive"""
    return {"status": "OK"}


@router.get(
    "/services",
    summary="Retrieve the configured services and the topics they subscribe to as a dict",
    status_code=status.HTTP_200_OK,
    response_model=dict[str, list[str]],
)
async def get_config(dlq_config: DLQConfigDependency):
    """Return the configured service abbreviations, each with a list of topics that
    the service subscribes to.
    """
    return dlq_config


@router.get(
    "/{service}/{topic}",
    summary="Return the next events in the topic",
    status_code=status.HTTP_200_OK,
)
async def get_events(
    service: str,
    topic: str,
    limit: int,
    skip: int,
    dlq_manager: DLQManagerDependency,
) -> list[EventInfo]:
    """Return the next events in the topic.

    This is a preview of the events and does not impact event ordering within the DLQ.
    """
    # TODO: error checking
    return await dlq_manager.preview_events(
        service=service, topic=topic, limit=limit, skip=skip
    )


@router.post(
    "/{service}/{topic}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def process_event(
    service: str,
    topic: str,
    dlq_manager: DLQManagerDependency,
    override: EventInfo | None = None,
) -> None:
    """Process the next event in the topic, optionally publishing the supplied event"""
    # TODO: error checking
    await dlq_manager.process_event(
        service=service, topic=topic, override=override, dry_run=False
    )


@router.post(
    "/{service}/{topic}/test",
    status_code=status.HTTP_200_OK,
)
async def process_event_dry_run(
    service: str,
    topic: str,
    dlq_manager: DLQManagerDependency,
    override: EventInfo | None = None,
) -> EventInfo | None:
    """Process the next event in the topic, optionally publishing the supplied event"""
    # TODO: error checking
    result = await dlq_manager.process_event(
        service=service, topic=topic, override=override, dry_run=True
    )

    return result


@router.delete(
    "/{service}/{topic}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def discard_event(
    service: str, topic: str, dlq_manager: DLQManagerDependency
) -> None:
    """Process the next event in the topic, optionally publishing the supplied event"""
    # TODO: error checking
    return await dlq_manager.discard_event(service=service, topic=topic)
