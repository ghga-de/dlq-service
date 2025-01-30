"""Module hosting the dependency injection logic."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, nullcontext

from fastapi import FastAPI

from dlqs.adapters.inbound.event_sub import DLQSubTranslator
from dlqs.adapters.inbound.fastapi_ import dummies
from dlqs.adapters.inbound.fastapi_.configure import get_configured_app
from dlqs.adapters.outbound.dao import EventDaoPort, get_aggregator, get_event_dao
from dlqs.adapters.outbound.event_pub import RetryPublisher
from dlqs.config import Config
from dlqs.core.dlq_manager import DLQManager
from dlqs.ports.inbound.dlq_manager import DLQManagerPort
from dlqs.ports.outbound.dao import AggregatorPort
from dlqs.ports.outbound.event_pub import RetryPublisherPort
from hexkit.providers.akafka.provider import KafkaEventPublisher, KafkaEventSubscriber
from hexkit.providers.mongodb import MongoDbDaoFactory


async def get_dao(*, config: Config) -> EventDaoPort:
    """Constructs and initializes the DAO factory."""
    dao_factory = MongoDbDaoFactory(config=config)
    dao = await get_event_dao(dao_factory=dao_factory)
    return dao


@asynccontextmanager
async def get_retry_publisher(
    *, config: Config
) -> AsyncGenerator[RetryPublisherPort, None]:
    """Constructs and initializes the retry publisher."""
    async with KafkaEventPublisher.construct(config=config) as publisher:
        yield RetryPublisher(publisher)


@asynccontextmanager
async def prepare_core(
    *,
    config: Config,
    dao_override: EventDaoPort | None = None,
    aggregator_override: AggregatorPort | None = None,
    retry_publisher_override: RetryPublisherPort | None = None,
) -> AsyncGenerator[DLQManagerPort, None]:
    """Constructs and initializes all core components and their outbound dependencies.

    The _override parameters can be used to override the default dependencies.
    """
    dao = dao_override or await get_dao(config=config)
    aggregator = aggregator_override or get_aggregator(config=config)

    async with (
        nullcontext(retry_publisher_override)
        if retry_publisher_override
        else get_retry_publisher(config=config) as retry_publisher
    ):
        yield DLQManager(
            config=config,
            retry_publisher=retry_publisher,
            dao=dao,
            aggregator=aggregator,
        )


def prepare_core_with_override(
    *, config: Config, dlq_manager_override: DLQManagerPort | None = None
):
    """Resolve the dlq_manager context manager based on config and override (if any)."""
    return (
        nullcontext(dlq_manager_override)
        if dlq_manager_override
        else prepare_core(config=config)
    )


@asynccontextmanager
async def prepare_rest_app(
    *,
    config: Config,
    dlq_manager_override: DLQManagerPort | None = None,
) -> AsyncGenerator[FastAPI, None]:
    """Construct and initialize an REST API app along with all its dependencies.
    By default, the core dependencies are automatically prepared but you can also
    provide them using the dlq_manager_override parameter.
    """
    app = get_configured_app(config=config)

    async with prepare_core_with_override(
        config=config,
        dlq_manager_override=dlq_manager_override,
    ) as dlq_manager:
        app.dependency_overrides[dummies.dlq_config_dummy] = lambda: config
        app.dependency_overrides[dummies.dlq_manager_port] = lambda: dlq_manager
        yield app


@asynccontextmanager
async def prepare_dlq_subscriber(
    *,
    config: Config,
    dlq_manager_override: DLQManagerPort | None = None,
) -> AsyncGenerator[KafkaEventSubscriber, None]:
    """Construct and initialize an event subscriber with all its dependencies.
    By default, the core dependencies are automatically prepared but you can also
    provide them using the override parameter.
    """
    async with prepare_core_with_override(
        config=config, dlq_manager_override=dlq_manager_override
    ) as dlq_manager:
        dlq_sub_translator = DLQSubTranslator(dlq_manager=dlq_manager)
        async with KafkaEventSubscriber.construct(
            config=config, translator=dlq_sub_translator
        ) as dlq_subscriber:
            yield dlq_subscriber
