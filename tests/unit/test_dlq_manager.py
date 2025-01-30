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

"""Unit tests for the DLQ Manager"""

from contextlib import nullcontext
from unittest.mock import AsyncMock

import pytest
from pytest import MonkeyPatch

from dlqs.core.dlq_manager import stored_event_from_dlq_event_info
from dlqs.inject import prepare_core
from dlqs.models import EventInfo, StoredDLQEvent
from dlqs.ports.inbound.dlq_manager import DLQManagerPort
from hexkit.correlation import new_correlation_id
from tests.fixtures import utils
from tests.fixtures.config import DEFAULT_CONFIG


def test_dlq_error_text():
    """Test the error text of all the DLQ error types"""
    event_id = "ufs,user-events,0,842"
    dlq_operation_error = DLQManagerPort.DLQOperationError()
    assert str(dlq_operation_error) == ""

    # Validation error
    dlq_validation_error = DLQManagerPort.DLQValidationError(
        event_id=event_id, reason="reason"
    )
    msg = f"Validation failed for DLQ Event '{event_id}': reason"
    assert str(dlq_validation_error) == msg

    # Deletion error
    dlq_deletion_error = DLQManagerPort.DLQDeletionError(event_id=event_id)
    msg = (
        f"Could not delete DLQ event '{event_id}' from the database."
        + " Maybe the event was already deleted or the database is unreachable."
    )
    assert str(dlq_deletion_error) == msg

    # Insertion error from duplicate entry
    dlq_insertion_error = DLQManagerPort.DLQInsertionError(
        event_id=event_id, already_exists=True
    )
    msg = (
        f"Could not insert DLQ event '{event_id}' into the database."
        + " Event with same ID already exists."
    )
    assert str(dlq_insertion_error) == msg

    # Insertion error from other reason
    dlq_insertion_error = DLQManagerPort.DLQInsertionError(
        event_id=event_id, already_exists=False
    )
    msg = f"Could not insert DLQ event '{event_id}' into the database."
    assert str(dlq_insertion_error) == msg


def test_stored_event_from_dlq_event_info():
    """Test the conversion of a DLQEventInfo object to a StoredDLQEvent object"""
    dlq_event = utils.get_graph_event(target="dlq", user_id1="user1", user_id2="user2")
    stored_dlq_event = stored_event_from_dlq_event_info(dlq_event)
    assert stored_dlq_event.service == "fss"
    assert stored_dlq_event.event_id == "fss,graph-updates,0,842"
    assert stored_dlq_event.topic == DEFAULT_CONFIG.kafka_dlq_topic
    assert stored_dlq_event.payload == dlq_event.payload
    assert stored_dlq_event.headers == dlq_event.headers
    assert stored_dlq_event.key == dlq_event.key
    assert stored_dlq_event.timestamp == dlq_event.timestamp
    assert isinstance(stored_dlq_event, StoredDLQEvent)


@pytest.mark.asyncio
async def test_process_override_different_cid(monkeypatch: MonkeyPatch):
    """Verify that the DLQManager prevents a user-supplied event from being
    published to the retry topic if the correlation ID does not match the one from
    the original event.
    """
    dlq_event = utils.get_graph_event(target="dlq", user_id1="user1", user_id2="user2")
    stored_dlq_event = stored_event_from_dlq_event_info(dlq_event)
    override_headers = dlq_event.headers.copy()
    override_headers["correlation_id"] = new_correlation_id()
    override_event = dlq_event.model_copy(update={"headers": override_headers})

    mock_agg = AsyncMock()
    mock_agg.aggregate.return_value = [stored_dlq_event]

    async with prepare_core(
        config=DEFAULT_CONFIG,
        dao_override=AsyncMock(),
        aggregator_override=mock_agg,
        retry_publisher_override=AsyncMock(),
    ) as dlq_manager:
        with pytest.raises(dlq_manager.DLQValidationError):
            await dlq_manager.process_event(
                service="fss",
                topic=dlq_event.topic,
                override=override_event,
                dry_run=False,
            )


@pytest.mark.asyncio
async def test_process_dry_run():
    """Ensure `process` doesn't actually publish the event if `dry_run` is True."""
    dlq_event = utils.get_graph_event(target="dlq", user_id1="user1", user_id2="user2")
    stored_dlq_event = stored_event_from_dlq_event_info(dlq_event)

    mock_dao = AsyncMock()
    mock_agg = AsyncMock()
    mock_agg.aggregate.return_value = [stored_dlq_event]
    mock_retry_publisher = AsyncMock()

    async with prepare_core(
        config=DEFAULT_CONFIG,
        dao_override=mock_dao,
        aggregator_override=mock_agg,
        retry_publisher_override=mock_retry_publisher,
    ) as dlq_manager:
        await dlq_manager.process_event(
            service="fss", topic=dlq_event.topic, override=None, dry_run=True
        )
        # Verify that the event was neither published nor deleted from the DLQ
        mock_dao.delete.assert_not_called()
        mock_retry_publisher.publish_event.assert_not_called()


@pytest.mark.asyncio
async def test_process_override_success():
    """Verify that the DLQManager successfully processes an event with an override."""
    dlq_event = utils.get_graph_event(target="dlq", user_id1="user1", user_id2="user2")
    stored_dlq_event = stored_event_from_dlq_event_info(dlq_event)

    # The event failed because the schema and type was outdated, so we update and retry
    payload = {
        "from_node_id": dlq_event.payload["source_id"],
        "to_node_id": dlq_event.payload["dest_id"],
    }
    override_event = dlq_event.model_copy(
        update={"payload": payload, "type_": "conn_added"}
    )

    mock_dao = AsyncMock()
    mock_agg = AsyncMock()
    mock_agg.aggregate.return_value = [stored_dlq_event]
    mock_retry_publisher = AsyncMock()

    async with prepare_core(
        config=DEFAULT_CONFIG,
        dao_override=mock_dao,
        aggregator_override=mock_agg,
        retry_publisher_override=mock_retry_publisher,
    ) as dlq_manager:
        published = await dlq_manager.process_event(
            service="fss", topic=dlq_event.topic, override=override_event, dry_run=False
        )
        # Verify that the event was published and deleted from the DLQ
        mock_dao.delete.assert_called_once_with(stored_dlq_event.event_id)
        mock_retry_publisher.send_to_retry_topic.assert_called_once_with(
            event=override_event, retry_topic=f"{utils.FSS}-retry"
        )
        assert published == override_event


@pytest.mark.parametrize(
    "override",
    [None, utils.get_graph_event(target="dlq", user_id1="user1", user_id2="user2")],
)
@pytest.mark.asyncio
async def test_process_with_empty_dlq(override: EventInfo | None):
    """Make sure no errors are raised and that `None` is returned when calling
    `process_event` with an empty DLQ. Behavior should be equal regardless of `override`.
    """
    mock_dao = AsyncMock()
    mock_agg = AsyncMock()
    mock_agg.aggregate.return_value = []
    mock_retry_publisher = AsyncMock()

    async with prepare_core(
        config=DEFAULT_CONFIG,
        dao_override=mock_dao,
        aggregator_override=mock_agg,
        retry_publisher_override=mock_retry_publisher,
    ) as dlq_manager:
        published = await dlq_manager.process_event(
            service="fss", topic=utils.USER_EVENTS, override=override, dry_run=False
        )
        assert not published
        mock_dao.delete.assert_not_called()
        mock_retry_publisher.send_to_retry_topic.assert_not_called()


@pytest.mark.parametrize(
    "skip, limit",
    [(0, 5), (5, 5), (10, 5), (15, 0)],
    ids=[
        "Beginning to Middle",
        "Middle to End",
        "Past end limited",
        "Past end unlimited",
    ],
)
@pytest.mark.asyncio
async def test_preview_pagination_valid_params(skip: int, limit: int):
    """Test that the preview pagination works as expected."""
    dlq_size = 10
    dlq_events = [
        utils.get_graph_event(target="dlq", user_id1=f"user{i}", user_id2=f"node{i}")
        for i in range(dlq_size)
    ]
    stored_dlq_events = [
        stored_event_from_dlq_event_info(event) for event in dlq_events
    ]

    async def _fake_agg(service: str, topic: str, skip: int, limit: int):
        return stored_dlq_events[skip : skip + limit]

    mock_agg = AsyncMock()
    mock_agg.aggregate = _fake_agg

    async with prepare_core(
        config=DEFAULT_CONFIG,
        dao_override=AsyncMock(),
        aggregator_override=mock_agg,
        retry_publisher_override=AsyncMock(),
    ) as dlq_manager:
        results = await dlq_manager.preview_events(
            service=utils.FSS, topic=utils.USER_EVENTS, limit=limit, skip=skip
        )
        assert results == dlq_events[skip : skip + limit]
        assert len(results) == min(limit, max(0, dlq_size - skip))


@pytest.mark.parametrize(
    "skip, limit",
    [(0, -5), (-5, -5), (-5, 0)],
    ids=["Negative Limit", "Both Negative", "Negative Skip"],
)
@pytest.mark.asyncio
async def test_preview_pagination_invalid_params(skip: int, limit: int):
    """Verify that preview_events with invalid skip/limit params raises a ValueError."""
    async with prepare_core(
        config=DEFAULT_CONFIG,
        dao_override=AsyncMock(),
        aggregator_override=AsyncMock(),
        retry_publisher_override=AsyncMock(),
    ) as dlq_manager:
        with pytest.raises(ValueError):
            _ = await dlq_manager.preview_events(
                service=utils.FSS, topic=utils.USER_EVENTS, limit=limit, skip=skip
            )


@pytest.mark.parametrize(
    "event_exists, error",
    [(True, False), (True, True), (False, False)],
    ids=["Event Exists", "Deletion Error", "Event Does Not Exist"],
)
@pytest.mark.asyncio
async def test_discard_event(event_exists: bool, error: bool):
    """Test what happens when we call `discard_event`.

    Should cover the following cases:
    - Event exists in the DLQ for the requested service
    - Event does not exist in the DLQ, but not for the requested service
    - Event exists but delete fails
    """
    dlq_event = utils.get_graph_event(target="dlq", user_id1="user1", user_id2="user2")
    stored_dlq_event = stored_event_from_dlq_event_info(dlq_event)

    async def _fake_agg(service: str, topic: str, skip: int, limit: int):
        return [stored_dlq_event] if event_exists else []

    mock_dao = AsyncMock()
    if error:
        mock_dao.delete.side_effect = DLQManagerPort.DLQDeletionError(
            event_id=stored_dlq_event.event_id
        )
    mock_agg = AsyncMock()
    mock_agg.aggregate = _fake_agg
    mock_retry_publisher = AsyncMock()
    service = utils.FSS if event_exists else utils.UFS
    async with prepare_core(
        config=DEFAULT_CONFIG,
        dao_override=mock_dao,
        aggregator_override=mock_agg,
        retry_publisher_override=mock_retry_publisher,
    ) as dlq_manager:
        with pytest.raises(dlq_manager.DLQDeletionError) if error else nullcontext():
            await dlq_manager.discard_event(service=service, topic=utils.USER_EVENTS)
        mock_retry_publisher.send_to_retry_topic.assert_not_called()

        if event_exists:
            mock_dao.delete.assert_called_once_with(stored_dlq_event.event_id)
        else:
            mock_dao.delete.assert_not_called()
