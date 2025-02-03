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
"""DLQ Manager Port definition"""

from abc import ABC, abstractmethod

from dlqs.models import EventInfo


class DLQManagerPort(ABC):
    """Manager for the Dead Letter Queue (DLQ) service"""

    class NotConfiguredError(RuntimeError):
        """Raised when the requested service & topic combination is not configured."""

        def __init__(self, *, service: str, topic: str):
            msg = (
                f"No matching configuration for service '{service}' and topic '{topic}'"
            )
            super().__init__(msg)

    class DLQValidationError(RuntimeError):
        """Raised when an event from the DLQ fails validation."""

        def __init__(self, *, event_id: str, reason: str):
            msg = f"Validation failed for DLQ Event '{event_id}': {reason}"
            super().__init__(msg)

    class DLQOperationError(RuntimeError):
        """Raised when an operation on the DLQ fails."""

    class DLQDeletionError(DLQOperationError):
        """Raised when an event could not be deleted from the DLQ."""

        def __init__(self, *, event_id: str):
            msg = (
                f"Could not delete DLQ event '{event_id}' from the database."
                + " Maybe the event was already deleted or the database is unreachable."
            )
            super().__init__(msg)

    class DLQInsertionError(DLQOperationError):
        """Raised when an event could not be inserted into the DLQ."""

        def __init__(self, *, event_id: str, already_exists: bool):
            msg = f"Could not insert DLQ event '{event_id}' into the database."
            if already_exists:
                msg += " Event with same ID already exists."
            super().__init__(msg)

    class DLQFetchNextError(DLQOperationError):
        """Raised when the next event from the DLQ could not be fetched."""

        def __init__(self, *, service: str, topic: str):
            msg = f"Failed to get next DLQ event for service '{service}' and topic '{topic}'"
            super().__init__(msg)

    class DLQPreviewError(DLQOperationError):
        """Raised when the next events from the DLQ could not be previewed."""

        def __init__(self, *, service: str, topic: str):
            msg = f"Failed to preview next DLQ events for service '{service}' and topic '{topic}'"
            super().__init__(msg)

    @abstractmethod
    async def store_event(self, *, event: EventInfo) -> None:
        """Store an event in the database with its service name and event ID.

        Raises a `DLQInsertionError` if the insertion fails.
        """
        ...

    @abstractmethod
    async def preview_events(
        self,
        *,
        service: str,
        topic: str,
        skip: int = 0,
        limit: int | None = None,
    ) -> list[EventInfo]:
        """Return a list of the next DLQ events for the given `service` and `topic`.

        Args:
        - `service`: The name of the service to preview events for.
        - `topic`: The name of the topic to preview.
        - `skip`: The number of events to skip for pagination. Default is 0.
        - `limit`: The maximum number of events to return. Default is 0 (no limit).

        Raises:
        - `ValueError` if there is a problem with the params supplied to the aggregator.
        - `DLQPreviewError` if the preview fails during the aggregation
        """
        ...

    @abstractmethod
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
        ...

    @abstractmethod
    async def discard_event(self, *, service: str, topic: str) -> None:
        """Discard (delete) the next DLQ event for the given `service` and `topic`.

        This operation skips validation and auto-processing, simply deleting the event.

        Raises:
        - `DLQFetchNextError` if the next event retrieval fails.
        - `DLQDeletionError` if the event could not be deleted.
        """
        ...
