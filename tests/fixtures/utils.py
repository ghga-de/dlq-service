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

"""Utils for Fixture handling."""

from typing import Literal

from dlqs.models import EventInfo
from hexkit.providers.akafka.provider.eventsub import (
    CORRELATION_ID_FIELD,
    EVENT_ID_FIELD,
    EXC_CLASS_FIELD,
    EXC_MSG_FIELD,
    ORIGINAL_TOPIC_FIELD,
)
from tests.fixtures.config import DEFAULT_CONFIG

# Service names
UFS = "ufs"
FSS = "fss"

# Topic names
NOTIFICATIONS = "notifications"
USER_EVENTS = "user-events"
GRAPH_UPDATES = "graph-updates"
DlqOrRetry = Literal["dlq", "retry"]
TEST_CID = "387a028c-a272-4086-b930-6d3e3c389d51"


def get_user_event(
    *,
    service: Literal["ufs", "fss"],
    target: DlqOrRetry,
    user_id: str,
) -> EventInfo:
    """Generate an event for the user-events topic, either for retry or dlq

    Works with either the UFS (user feed service) or the FSS (friend suggestion service)
    """
    topic = DEFAULT_CONFIG.kafka_dlq_topic if target == "dlq" else f"{service}-retry"
    headers = {ORIGINAL_TOPIC_FIELD: USER_EVENTS}
    if target == "dlq":
        headers[EVENT_ID_FIELD] = f"{service},{USER_EVENTS},0,370"
        headers[EXC_CLASS_FIELD] = "ResourceAlreadyExistsError"
        headers[EXC_MSG_FIELD] = f'The resource with the id "{user_id}" already exists.'
        headers[CORRELATION_ID_FIELD] = TEST_CID

    dlq_user_event = EventInfo(
        topic=topic,
        payload={"user_id": user_id, "name": "John Doe"},
        type_="registration",
        key=user_id,
        headers=headers,
    )
    return dlq_user_event


def get_notifications_event(
    *,
    target: DlqOrRetry,
    user_id1: str,
    user_id2: str,
) -> EventInfo:
    """Generate an event for the notifications topic, either for retry or dlq.

    Only for the UFS (user feed service).
    """
    topic = DEFAULT_CONFIG.kafka_dlq_topic if target == "dlq" else "ufs-retry"
    headers = {ORIGINAL_TOPIC_FIELD: NOTIFICATIONS}
    if target == "dlq":
        headers[EVENT_ID_FIELD] = f"{UFS}{NOTIFICATIONS},1,164"
        headers[EXC_CLASS_FIELD] = "ResourceNotFoundError"
        headers[EXC_MSG_FIELD] = (
            f'The resource with the id "{user_id2}" does not exist.'
        )
        headers[CORRELATION_ID_FIELD] = TEST_CID

    notifications_event = EventInfo(
        topic=topic,
        type_="tagged_in_photo",
        payload={
            "photo_id": "76dc2b15-38e5-4e67-9ee9-7da031d7fc45",
            "tagger_id": user_id1,
            "tagged_id": user_id2,
            "msg": "You were tagged in a photo",
        },
        key=user_id2,
        headers=headers,
    )
    return notifications_event


def get_graph_event(
    *,
    target: DlqOrRetry,
    user_id1: str,
    user_id2: str,
) -> EventInfo:
    """Generate an event for the graph-updates topic, either for retry or dlq.

    Only for the FSS (friend suggestion service).
    """
    topic = DEFAULT_CONFIG.kafka_dlq_topic if target == "dlq" else "fss-retry"
    headers = {ORIGINAL_TOPIC_FIELD: GRAPH_UPDATES}
    if target == "dlq":
        headers[EVENT_ID_FIELD] = f"{FSS},{GRAPH_UPDATES},0,842"
        headers[EXC_CLASS_FIELD] = "ResourceNotFoundError"
        headers[EXC_MSG_FIELD] = (
            f'The resource with the id "{user_id2}" does not exist.'
        )
        headers[CORRELATION_ID_FIELD] = TEST_CID

    graph_event = EventInfo(
        topic=topic,
        type_="connection_added",
        key=user_id1,
        payload={"source_id": user_id1, "dest_id": user_id2},
        headers=headers,
    )
    return graph_event
