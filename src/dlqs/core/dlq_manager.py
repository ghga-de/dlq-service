# Copyright 2021 - 2024 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Domain logic for the DLQ Service"""

import logging

from dlqs.config import Config
from dlqs.models import EventInfo, StoredDLQEvent
from dlqs.ports.inbound.dlq_manager import DLQManagerPort
from dlqs.ports.outbound.dao import AggregatorPort, EventDaoPort
from dlqs.ports.outbound.event_pub import RetryPublisherPort
from hexkit.correlation import set_correlation_id
from hexkit.protocols.dao import ResourceAlreadyExistsError
from hexkit.providers.akafka.provider.eventsub import (
    EVENT_ID_FIELD,
    ORIGINAL_TOPIC_FIELD,
)

log = logging.getLogger(__name__)


def stored_event_from_dlq_event_info(event: EventInfo) -> StoredDLQEvent:
    """Convert a DLQEventInfo object to a StoredDLQEvent object"""
    event_id = event.headers[EVENT_ID_FIELD]
    service = event_id.split(",")[0]
    event_dict = event.model_dump()
    stored_event = StoredDLQEvent(service=service, event_id=event_id, **event_dict)
    return stored_event


class DLQManager(DLQManagerPort):
    """Manager for the Dead Letter Queue (DLQ) service"""

    def __init__(
        self,
        config: Config,
        retry_publisher: RetryPublisherPort,
        dao: EventDaoPort,
        aggregator: AggregatorPort,
    ):
        self._config = config
        self._retry_publisher = retry_publisher
        self._dao = dao
        self._aggregator = aggregator

    async def _delete_event(self, *, event_id: str) -> None:
        """Delete the event with the given `event_ID` from the database.

        Raises a `DLQDeletionError` if the event could not be found.
        The error is raised because in this service we always retrieve the event
        before deleting it.
        """
        try:
            await self._dao.delete(event_id)
            log.debug("Deleted DLQ event with '%s'", event_id)
        except Exception as err:
            error = self.DLQDeletionError(event_id=event_id)
            log.error(error)
            raise error from err

    async def _get_next_event(
        self, *, service: str, topic: str
    ) -> StoredDLQEvent | None:
        """Get the next event from the DLQ for the given `service` and `topic`.

        Returns the next event or `None` if no events are found.

        Raises a `DLQFetchNextError` if the operation fails.
        """
        try:
            events = await self._aggregator.aggregate(
                service=service, topic=topic, skip=0, limit=1
            )
            if events:
                return events[0]
        except Exception as err:
            error = self.DLQFetchNextError(service=service, topic=topic)
            log.error(error)
            raise error from err

        log.debug("No DLQ events found for service %s and topic %s", service, topic)
        return None

    def _validate_event(
        self, *, stored_event: StoredDLQEvent, to_publish: EventInfo
    ) -> None:
        """Validate the event before processing it.

        Ensures that:
        - The correlation ID is set and matches the stored event.
        - The topic field is not set to a retry topic.

        Raises a `ValueError` if any of the above conditions are not met.
        """
        # Correlation ID must be the same
        if not to_publish.headers["correlation_id"]:
            raise ValueError("Correlation ID not set")

        # Assume stored DLQ events always have a correlation ID (they should)
        expected_cid = stored_event.headers["correlation_id"]
        actual_cid = to_publish.headers["correlation_id"]
        if actual_cid != expected_cid:
            raise ValueError(
                f"Correlation ID mismatch. Expected {expected_cid}, got {actual_cid}"
            )

        # Published event can't have DLQ or '-retry' in the topic
        if to_publish.topic.endswith("-retry"):
            raise ValueError(
                f"Resolved event topic can't be set to '{to_publish.topic}'"
            )

    async def store_event(self, *, event: EventInfo) -> None:
        """Store an event in the database with its service name and event ID.

        Raises a `DLQInsertionError` if the insertion fails.
        """
        stored_event = stored_event_from_dlq_event_info(event)
        try:
            await self._dao.insert(stored_event)
        except Exception as err:
            error = self.DLQInsertionError(
                event_id=stored_event.event_id,
                already_exists=isinstance(err, ResourceAlreadyExistsError),
            )
            log.error(error)
            raise error from err

    async def preview_events(
        self,
        *,
        service: str,
        topic: str,
        skip: int,
        limit: int,
    ) -> list[EventInfo]:
        """Return a list of the next DLQ events for the given `service` and `topic`.

        Args:
        - `service`: The name of the service to preview events for.
        - `topic`: The name of the topic to preview.
        - `skip`: The number of events to skip for pagination.
        - `limit`: The maximum number of events to return for pagination.

        Raises:
        - `ValueError` if `skip` and/or `limit` are less than 0.
        - `DLQPreviewError` if the preview fails during the aggregation.
        """
        if limit < 0 or skip < 0:
            raise ValueError(
                f"Skip and limit must be 0 or greater. Got {skip=}, {limit=}"
            )

        try:
            events = await self._aggregator.aggregate(
                service=service, topic=topic, skip=skip, limit=limit
            )
        except AggregatorPort.AggregationError as err:
            error = self.DLQPreviewError(service=service, topic=topic)
            log.error(error)
            raise error from err

        return [EventInfo(**event.model_dump()) for event in events]

    async def process_event(
        self, *, service: str, topic: str, override: EventInfo | None, dry_run: bool
    ) -> EventInfo | None:
        """Process the next event from the DLQ for the given `service` and `topic`.

        Args:
        - `service`: The service name for the DLQ to process.
        - `topic`: The topic name for the DLQ to process.
        - `override`: An optional event to publish instead of the next event.
        - `dry_run`: Whether to actually publish the event to the retry topic.

        Returns the event that was or would be published (for dry-runs) else `None`.

        Raises:
        - `DLQFetchNextError` if the next event retrieval fails.
        - `DLQValidationError` if the event fails validation.
        - `DLQDeletionError` if the event could not be deleted from the DB after processing.
        """
        next_event = await self._get_next_event(service=service, topic=topic)
        if not next_event:
            return None

        event_id = next_event.event_id
        to_publish = override if override else next_event

        # Perform event validation
        try:
            self._validate_event(stored_event=next_event, to_publish=to_publish)
        except ValueError as err:
            dlq_error = self.DLQValidationError(event_id=event_id, reason=str(err))
            log.error(dlq_error)
            raise dlq_error from err

        # The only header required for events to be retried is the original topic header
        to_publish.headers = {ORIGINAL_TOPIC_FIELD: topic}

        # For dry-runs, only return the event to be published
        if dry_run:
            return to_publish

        # If this isn't a dry-run, actually publish the event to the retry topic
        async with set_correlation_id(next_event.headers["correlation_id"]):
            await self._retry_publisher.send_to_retry_topic(
                event=to_publish, retry_topic=f"{service}-retry"
            )

        # Remove the resolved event from the database
        await self._delete_event(event_id=event_id)
        return to_publish

    async def discard_event(self, *, service: str, topic: str) -> None:
        """Discard (delete) the next DLQ event for the given `service` and `topic`.

        This operation skips validation and auto-processing, simply deleting the event.

        Raises:
        - `DLQFetchNextError` if the next event retrieval fails.
        - `DLQDeletionError` if the event could not be deleted.
        """
        next_event = await self._get_next_event(service=service, topic=topic)
        if not next_event:
            return

        event_id = next_event.event_id

        await self._delete_event(event_id=event_id)
        log.info(
            "Discarded next DLQ event for %s's '%s' topic (event ID was '%s')",
            service,
            topic,
            event_id,
        )
