"""Utils to configure the FastAPI app"""

from typing import Any

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from ghga_service_commons.api import ApiConfigBase, configure_app

from dlqs import __version__
from dlqs.adapters.inbound.fastapi_.routes import router


def get_openapi_schema(app: FastAPI, config: ApiConfigBase) -> dict[str, Any]:
    """Generate a custom OpenAPI schema for the service."""
    return get_openapi(
        title="DLQ Service",
        version=__version__,
        description="A service to manage the dead letter queue for Kafka events",
        servers=[{"url": config.api_root_path}],
        routes=app.routes,
    )


def get_configured_app(config: ApiConfigBase) -> FastAPI:
    """Create and configure a REST API application."""
    app = FastAPI()
    app.include_router(router)
    configure_app(app, config=config)

    def custom_openapi():
        if app.openapi_schema:
            return app.openapi_schema
        openapi_schema = get_openapi_schema(app, config=config)
        app.openapi_schema = openapi_schema
        return app.openapi_schema

    app.openapi = custom_openapi  # type: ignore [method-assign]

    return app
