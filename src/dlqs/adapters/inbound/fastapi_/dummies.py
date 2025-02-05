"""Dependency dummies for the FastAPI endpoints"""

from typing import Annotated

from fastapi import Depends
from ghga_service_commons.api.di import DependencyDummy

from dlqs.ports.inbound.dlq_manager import DLQManagerPort

dlq_manager_port = DependencyDummy("dlq_manager_port")

DLQManagerDependency = Annotated[DLQManagerPort, Depends(dlq_manager_port)]
