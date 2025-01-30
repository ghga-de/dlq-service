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

"""Integration tests centered on the DlqManager class that use MongoDb and Kafka fixtures"""

import pytest

from dlqs.models import EventInfo
from hexkit.correlation import set_new_correlation_id
from tests.fixtures import utils
from tests.fixtures.joint import JointFixture

pytestmark = pytest.mark.asyncio()


def make_user_id(i: int) -> str:
    """Returns 'user_id' appended with the given integer"""
    return f"user_id{i}"


async def test_discard(joint_fixture: JointFixture):
    """Verify that we can discard the next event in a topic."""
    # Publish events to the user-events dlq topic for the UFS
    uff_events_published: list[EventInfo] = []
    for i in range(2):
        user_id = make_user_id(i)
        dlq_event = utils.get_user_event(service="ufs", target="dlq", user_id=user_id)
        async with set_new_correlation_id() as correlation_id:
            dlq_event.headers["correlation_id"] = correlation_id
            uff_events_published.append(dlq_event)
            await joint_fixture.kafka.publish_event(**vars(dlq_event))

    # Publish events to the user-events dlq topic for the FSS
    fss_event = utils.get_user_event(
        service="fss", target="dlq", user_id=make_user_id(0)
    )
    async with set_new_correlation_id() as correlation_id:
        fss_event.headers["correlation_id"] = correlation_id
        await joint_fixture.kafka.publish_event(**vars(fss_event))

    # Discard the next event in the user-events dlq topic for the UFS
    await joint_fixture.dlq_manager.discard_event(
        service="ufs", topic=utils.USER_EVENTS
    )

    # Preview events and verify that the latest event is gone from the UFS dlq
    preview = await joint_fixture.dlq_manager.preview_events(
        service="ufs", topic=utils.USER_EVENTS, limit=10, skip=0
    )
    assert len(preview) == 1
    assert preview == uff_events_published[1:]

    # Make sure the FSS dlq is unaffected
    preview = await joint_fixture.dlq_manager.preview_events(
        service="fss", topic=utils.USER_EVENTS, limit=10, skip=0
    )
    assert len(preview) == 1
    assert preview == [fss_event]

    # Discard the next event in the user-events dlq topic for the FSS
    await joint_fixture.dlq_manager.discard_event(
        service="fss", topic=utils.USER_EVENTS
    )


async def test_preview(joint_fixture: JointFixture):
    """Test the preview functionality of the DLQ manager.

    Publishes events to the topic shared by the UFS and FSS services, then
    previews them. This test should cover:
    - Preview repeatability (idempotence)
    - Error handling
    - Previewing events from different services and topics
    - Previewing events with different limits and skips
    """
    # Publish events to the user-events dlq topic for the UFS
    ufs_user_events: list[EventInfo] = []
    for i in range(2):
        user_id = make_user_id(i)
        dlq_event = utils.get_user_event(service="ufs", target="dlq", user_id=user_id)
        async with set_new_correlation_id() as correlation_id:
            dlq_event.headers["correlation_id"] = correlation_id
            ufs_user_events.append(dlq_event)
            await joint_fixture.kafka.publish_event(**vars(dlq_event))

    # Publish events to the user-events dlq topic for the FSS
    fss_user_events: list[EventInfo] = []
    for i in range(2):
        user_id = make_user_id(i)
        dlq_event = utils.get_user_event(service="fss", target="dlq", user_id=user_id)
        async with set_new_correlation_id() as correlation_id:
            dlq_event.headers["correlation_id"] = correlation_id
            fss_user_events.append(dlq_event)
            await joint_fixture.kafka.publish_event(**vars(dlq_event))

    # Preview events and verify they match what was published
    ufs_users_preview = await joint_fixture.dlq_manager.preview_events(
        service="ufs", topic=utils.USER_EVENTS, limit=10, skip=0
    )
    assert ufs_users_preview == ufs_user_events

    fss_users_preview = await joint_fixture.dlq_manager.preview_events(
        service="fss", topic=utils.USER_EVENTS, limit=10, skip=0
    )
    assert fss_users_preview == fss_user_events


@pytest.mark.parametrize(
    "service,topic", [(utils.UFS, "faketopic"), ("fakeservice", utils.USER_EVENTS)]
)
async def test_not_configured_error(
    joint_fixture: JointFixture, service: str, topic: str
):
    """Verify that NotConfiguredError is raised when there is no KafkaDLQSubscriber
    for the DLQ topic of the given service and topic.
    """
    # Call preview
    with pytest.raises(joint_fixture.dlq_manager.NotConfiguredError):
        await joint_fixture.dlq_manager.preview_events(
            service=service, topic=topic, limit=10, skip=0
        )

    # Call discard
    with pytest.raises(joint_fixture.dlq_manager.NotConfiguredError):
        await joint_fixture.dlq_manager.discard_event(service=service, topic=topic)

    # Call process
    with pytest.raises(joint_fixture.dlq_manager.NotConfiguredError):
        await joint_fixture.dlq_manager.process_event(
            service=service, topic=topic, override=None, dry_run=False
        )
