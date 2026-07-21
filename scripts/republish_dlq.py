#!/usr/bin/env python3

# Copyright 2021 - 2026 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""Republishes all events currently sitting in the DLQ service.

Events must be republished one at a time, in order, per service/topic queue
(the API rejects a dlq_id that isn't next in line with a 409). This script
walks the /summary endpoint to find every service/topic queue, then drains
each one by repeatedly previewing the head event and POSTing it back.
"""

import argparse
import os
import sys

import requests

DEFAULT_BASE_URL = "http://localhost:8080"
TOKEN_ENV_VAR = "DLQS_TOKEN"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"Base URL of the DLQ service (default: {DEFAULT_BASE_URL})",
    )
    parser.add_argument(
        "--token",
        default=os.environ.get(TOKEN_ENV_VAR),
        help=f"Bearer token for the DLQ service API (default: read from "
        f"${TOKEN_ENV_VAR})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be republished without actually publishing",
    )
    parser.add_argument(
        "--service",
        help="Only republish events for this service (requires --topic)",
    )
    parser.add_argument(
        "--topic",
        help="Only republish events for this topic (requires --service)",
    )
    args = parser.parse_args()
    if not args.token:
        parser.error(
            f"A bearer token is required. Pass --token or set ${TOKEN_ENV_VAR}."
        )
    if bool(args.service) != bool(args.topic):
        parser.error("--service and --topic must be given together.")
    return args


def get_summary(session: requests.Session, base_url: str) -> dict[str, dict[str, int]]:
    """Fetch the service/topic/count breakdown of the DLQ."""
    response = session.get(f"{base_url}/summary")
    response.raise_for_status()
    return response.json()


def get_next_dlq_id(
    session: requests.Session, base_url: str, service: str, topic: str
) -> str | None:
    """Preview the head event of a service/topic queue and return its dlq_id."""
    response = session.get(f"{base_url}/{service}/{topic}", params={"limit": 1})
    response.raise_for_status()
    events = response.json()
    return events[0]["dlq_id"] if events else None


def preview_dlq_ids(
    session: requests.Session, base_url: str, service: str, topic: str, limit: int
) -> list[str]:
    """Preview up to `limit` events in a service/topic queue and return their dlq_ids.

    Unlike republishing, previewing doesn't remove events from the queue, so this
    is safe to use to list an entire batch at once (e.g. for --dry-run).
    """
    response = session.get(f"{base_url}/{service}/{topic}", params={"limit": limit})
    response.raise_for_status()
    return [event["dlq_id"] for event in response.json()]


def republish_event(
    session: requests.Session, base_url: str, service: str, topic: str, dlq_id: str
) -> None:
    """Republish the given DLQ event."""
    response = session.post(
        f"{base_url}/{service}/{topic}",
        params={"dry_run": "false"},
        json={"dlq_id": dlq_id},
    )
    response.raise_for_status()


def drain_queue(
    session: requests.Session,
    base_url: str,
    service: str,
    topic: str,
    count: int,
    dry_run: bool,
) -> int:
    """Republish every event in a single service/topic queue, in order.

    Dry-run mode never actually removes events from the queue, so the head event
    would never advance if we looped on get_next_dlq_id like the real run does.
    Instead, just preview the whole batch up front and report it.
    """
    if dry_run:
        dlq_ids = preview_dlq_ids(session, base_url, service, topic, count)
        for preview_id in dlq_ids:
            print(f"  Would republish {service}/{topic} dlq_id={preview_id}")
        return 0

    republished = 0
    for _ in range(count):
        dlq_id = get_next_dlq_id(session, base_url, service, topic)
        if dlq_id is None:
            break
        print(f"  Republishing {service}/{topic} dlq_id={dlq_id}")
        republish_event(session, base_url, service, topic, dlq_id)
        republished += 1
    return republished


def main() -> int:
    args = parse_args()
    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {args.token}"

    summary = get_summary(session, args.base_url)
    if args.service:
        count = summary.get(args.service, {}).get(args.topic, 0)
        summary = {args.service: {args.topic: count}} if count else {}

    total_events = sum(
        count for topics in summary.values() for count in topics.values()
    )
    if not total_events:
        print("DLQ is empty, nothing to republish.")
        return 0

    print(f"Found {total_events} failed event(s) across {len(summary)} service(s).")
    if args.dry_run:
        print("Running in --dry-run mode; no events will actually be published.\n")

    total_republished = 0
    try:
        for service, topics in summary.items():
            for topic, count in topics.items():
                print(f"{service}/{topic}: {count} event(s)")
                total_republished += drain_queue(
                    session, args.base_url, service, topic, count, args.dry_run
                )
    except requests.HTTPError as err:
        url = err.request.url if err.request is not None else "unknown URL"
        print(f"\nAborting: request to {url!r} failed: {err}", file=sys.stderr)
        if err.response is not None:
            print(err.response.text, file=sys.stderr)
        return 1

    if args.dry_run:
        print(f"\nDry run complete. {total_events} event(s) would be republished.")
    else:
        print(f"\nDone. Republished {total_republished} event(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
