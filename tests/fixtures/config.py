# Copyright 2021 - 2025 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""Test config"""

from pathlib import Path

from pydantic_settings import BaseSettings

from dlqs.config import Config

BASE_DIR = Path(__file__).parent.resolve()
TEST_CONFIG_YAML = BASE_DIR / "test_config.yaml"


def get_config(
    sources: list[BaseSettings] | None = None,
    default_config_yaml: Path = TEST_CONFIG_YAML,
    **kwargs,
) -> Config:
    """Merges parameters from the default TEST_CONFIG_YAML with params inferred
    from testcontainers.
    """
    sources_dict: dict[str, object] = {}

    if sources is not None:
        for source in sources:
            sources_dict.update(**source.model_dump())
    sources_dict.update(**kwargs)

    return Config(config_yaml=default_config_yaml, **sources_dict)  # type: ignore


DEFAULT_CONFIG = get_config()
