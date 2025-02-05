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

"""Event publish translator targeting the EventPublisherProtocol"""

import logging

from hexkit.protocols.eventpub import EventPublisherProtocol

from dlqs.models import EventInfo
from dlqs.ports.outbound.event_pub import RetryPublisherPort

log = logging.getLogger(__name__)


class RetryPublisher(RetryPublisherPort):
    """Kafka event publisher that sends events to the 'retry' topics"""

    def __init__(self, publisher: EventPublisherProtocol):
        self._publisher = publisher

    async def send_to_retry_topic(self, *, event: EventInfo, retry_topic: str) -> None:
        """Publish the given event to the appropriate 'retry' topic"""
        await self._publisher.publish(
            topic=retry_topic,
            payload=event.payload,
            type_=event.type_,
            key=event.key,
            headers=event.headers,
        )
        log.debug("Published event to retry topic %s", retry_topic)
