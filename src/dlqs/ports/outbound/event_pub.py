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

"""Outbound port for sending events to the 'retry' topics."""

from abc import ABC, abstractmethod

from dlqs.models import EventInfo


class RetryPublisherPort(ABC):
    """Port for an event publisher that sends events to the 'retry' topics"""

    @abstractmethod
    async def send_to_retry_topic(self, *, event: EventInfo, retry_topic: str) -> None:
        """Publish the given event to the appropriate 'retry' topic"""
        ...
