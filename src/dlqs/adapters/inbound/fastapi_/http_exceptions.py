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

"""Reusable HTTP response definitions"""

from fastapi import status
from ghga_service_commons.httpyexpect.server.exceptions import HttpCustomExceptionBase

from dlqs.models import EventInfo


class HttpInternalServerError(HttpCustomExceptionBase):
    """Thrown when an error is raised with details that should not be propagated to a client"""

    exception_id = "internalServerError"

    def __init__(self, *, status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR):
        """Construct message and init the exception."""
        super().__init__(
            status_code=status_code, description="Internal Server Error.", data={}
        )


class HttpPreviewParamsError(HttpCustomExceptionBase):
    """Thrown when an error is raised because of invalid values for `skip` or `limit`"""

    exception_id = "previewParamsError"

    def __init__(
        self,
        *,
        status_code: int = status.HTTP_400_BAD_REQUEST,
        skip: int,
        limit: int | None,
    ):
        """Construct message and init the exception."""
        super().__init__(
            status_code=status_code,
            description="Invalid values for `skip` or `limit`.",
            data={skip: skip, limit: limit},
        )


class HttpOverrideValidationError(HttpCustomExceptionBase):
    """Thrown when a manually supplied override event does not pass validation"""

    exception_id = "overrideValidationError"

    def __init__(
        self,
        *,
        status_code: int = status.HTTP_400_BAD_REQUEST,
        event: EventInfo | None,
        reason: str,
    ):
        """Construct message and init the exception."""
        super().__init__(
            status_code=status_code,
            description="Event validation failed.",
            data={"event": event.model_dump() if event else {}, "reason": reason},
        )


class HttpDiscardError(HttpCustomExceptionBase):
    """Thrown when the discard operation fails"""

    exception_id = "discardError"

    def __init__(self, *, status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR):
        """Construct message and init the exception."""
        super().__init__(
            status_code=status_code,
            description="Discard operation failed, try again.",
            data={},
        )
