"""FastAPI endpoint function definitions"""

from fastapi import APIRouter, status
from pydantic import UUID4

from dlqs import models
from dlqs.adapters.inbound.fastapi_.dummies import DLQManagerDependency
from dlqs.adapters.inbound.fastapi_.http_exceptions import (
    HttpDiscardError,
    HttpInternalServerError,
    HttpOverrideValidationError,
    HttpPreviewParamsError,
)

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
    "/{service}/{topic}",
    summary="Return the next events in the topic",
    status_code=status.HTTP_200_OK,
)
async def get_events(
    dlq_manager: DLQManagerDependency,
    service: str,
    topic: str,
    skip: int = 0,
    limit: int | None = None,
) -> list[models.StoredDLQEvent]:
    """Return the next events in the topic.

    This is a preview of the events and does not impact event ordering within the DLQ.
    """
    try:
        return await dlq_manager.preview_events(
            service=service, topic=topic, limit=limit, skip=skip
        )
    except ValueError as err:
        raise HttpPreviewParamsError(skip=skip, limit=limit) from err
    except Exception as exc:
        # DLQPreviewError and all others get caught here
        raise HttpInternalServerError() from exc


@router.post(
    "/{service}/{topic}",
    status_code=status.HTTP_200_OK,
)
async def process_event(  # noqa: PLR0913
    service: str,
    topic: str,
    dlq_manager: DLQManagerDependency,
    dlq_id: UUID4,
    override: models.EventCore | None = None,
    dry_run: bool = False,
) -> models.PublishableEventData | None:
    """Process the next event in the topic, optionally publishing the supplied event"""
    # TODO: update doc string
    try:
        return await dlq_manager.process_event(
            service=service,
            topic=topic,
            dlq_id=dlq_id,
            override=override,
            dry_run=dry_run,
        )
    except dlq_manager.DLQValidationError as err:
        raise HttpOverrideValidationError(event=override, reason=str(err)) from err
    except Exception as exc:
        # lump all other errors here
        raise HttpInternalServerError() from exc


@router.delete(
    "/{dlq_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def discard_event(dlq_id: UUID4, dlq_manager: DLQManagerDependency) -> None:
    """Process the next event in the topic, optionally publishing the supplied event"""
    # TODO: update doc string
    try:
        return await dlq_manager.discard_event(dlq_id=dlq_id)
    except dlq_manager.DLQDeletionError as err:
        raise HttpDiscardError() from err
    except Exception as exc:
        # lump all other errors here
        raise HttpInternalServerError() from exc
