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

"""REST API-focused unit tests for the  that mock the DlqManager"""

from unittest.mock import AsyncMock

import pytest
from ghga_service_commons.api.testing import AsyncTestClient

from dlqs.inject import prepare_rest_app
from tests.fixtures import utils
from tests.fixtures.config import DEFAULT_CONFIG


@pytest.mark.asyncio
async def test_preview_bad_params():
    """Test how ValueError is translated (from bad `skip` or `limit` args)"""
    dlq_manager = AsyncMock()
    dlq_manager.preview_events.side_effect = ValueError("bad params")
    async with (
        prepare_rest_app(
            config=DEFAULT_CONFIG,
            dlq_manager_override=dlq_manager,
        ) as app,
        AsyncTestClient(app=app) as client,
    ):
        response = await client.get(f"/{utils.UFS}/{utils.USER_EVENTS}?skip=-1&limit=0")
        assert response.status_code == 400
        assert response.json() == {
            "description": (
                "Invalid values for `skip` and/or `limit`. Skip must be >=0 if supplied"
                + " and limit must be >=1 if supplied."
            ),
            "data": {"skip": -1, "limit": 0},
            "exception_id": "previewParamsError",
        }
