import datetime as dt
import json
from pathlib import Path
from typing import Callable
from uuid import uuid4

import boto3
import polling2
import pytest
import requests
from db.models.granule import Granule
from mypy_boto3_sqs import SQSClient
from sqlalchemy.orm import Session


def check_sqs_message_count(sqs_client, queue_url, count):
    queue_attributes = sqs_client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"]
    )
    return int(queue_attributes["Attributes"]["ApproximateNumberOfMessages"]) == count


def _format_dt(datetime: dt.datetime) -> str:
    """Format datetime into string used by ESA's payload"""
    return datetime.isoformat().replace("+00:00", "Z")


@pytest.fixture
def recent_event_s2_created() -> dict:
    """Create a recent Sentinel-2 "Created" event from ESA's push subscription

    This message contains two types of fields,
    * Message metadata (event type, subscription ID, ack ID, notification date, etc)
    * Message "body" - `(.value)`
    """
    # Reusing example from ESA as a template
    data = (
        Path(__file__).parents[1]
        / "lambdas"
        / "link_fetcher"
        / "tests"
        / "data"
        / "push-granule-created-s2-n1.json"
    )
    with data.open() as src:
        payload = json.load(src)

    # Update relevant parts of message payload to be "recent"
    # where recent is <30 days from today as we're not currently
    # reprocessing historical scenes that ESA has reprocessed
    now = dt.datetime.now(tz=dt.timezone.utc)

    payload["NotificationDate"] = _format_dt(now)
    payload["value"]["OriginDate"] = _format_dt(now - dt.timedelta(seconds=7))
    payload["value"]["PublicationDate"] = _format_dt(now - dt.timedelta(seconds=37))
    payload["value"]["ModificationDate"] = _format_dt(now - dt.timedelta(seconds=1))
    payload["value"]["ContentDate"] = {
        "Start": _format_dt(now - dt.timedelta(hours=3, seconds=3)),
        "End": _format_dt(now - dt.timedelta(hours=3)),
    }
    # We're not using fields in `payload["value"]["Attributes"]` but there's duplicate
    # datetime information in there following OData conventions

    # Randomize ID of message to ensure each fixture's return is unique according
    # to our DB (which uses granule ID as primary key)
    payload["value"]["Id"] = str(uuid4())

    return payload


@pytest.fixture
def link_subscription_credentials(
    identifier: str, ssm_parameter: Callable[[str], str]
) -> tuple[str, str]:
    """Return user/pass credentials for subscription endpoint"""
    secrets_manager_client = boto3.client("secretsmanager")
    secret = json.loads(
        secrets_manager_client.get_secret_value(
            SecretId=(
                f"hls-s2-downloader-serverless/{identifier}/esa-subscription-credentials"
            )
        )["SecretString"]
    )

    return (
        secret["notification_username"],
        secret["notification_password"],
    )


@pytest.mark.parametrize("notification_count", [1, 2])
def test_link_push_subscription_handles_event(
    recent_event_s2_created: dict,
    link_subscription_endpoint_url: str,
    link_subscription_credentials: tuple[str, str],
    db_session: Session,
    sqs_client: SQSClient,
    queue_url: str,
    notification_count: int,
):
    """Test that we handle a new granule created notification

    We have occasionally observed duplicate granule IDs being
    sent to our API endpoint and we want to only process one,
    so this test includes a parametrized "notification_count"
    to replicate this reality.
    """
    for _ in range(notification_count):
        resp = requests.post(
            f"{link_subscription_endpoint_url}events",
            auth=link_subscription_credentials,
            json=recent_event_s2_created,
        )

        # ensure correct response (204)
        assert resp.status_code == 204

    # ensure we have SQS message
    polling2.poll(
        check_sqs_message_count,
        args=(sqs_client, queue_url, 1),
        step=5,
        timeout=120,
    )

    # ensure we have 1 granule for this ID
    granules = (
        db_session.query(Granule).filter(
            Granule.id == recent_event_s2_created["value"]["Id"]
        )
    ).all()
    assert len(granules) == 1


def test_link_push_subscription_user_auth_rejects_incorrect(
    link_subscription_endpoint_url: str,
):
    """Test that we reject incorrect authentication"""
    url = f"{link_subscription_endpoint_url}events"
    print(f"Sending POST request to {url=}")
    resp = requests.post(
        url,
        auth=(
            "foo",
            "bar",
        ),
        json={},
    )

    # ensure correct response (401 Unauthorized)
    assert resp.status_code == 401
