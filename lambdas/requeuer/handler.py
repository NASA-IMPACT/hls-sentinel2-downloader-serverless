from __future__ import annotations

import datetime
import json
import os
from typing import TYPE_CHECKING, Any, Callable, Mapping, Sequence, TypedDict

import iso8601
from db.models.granule import Granule
from sqlalchemy import func  # type: ignore
from sqlalchemy.orm import Session  # type: ignore
from typing_extensions import TypeAlias

if TYPE_CHECKING:
    from mypy_boto3_sqs import SQSClient

SessionMaker: TypeAlias = Callable[[], Session]


class GranuleMessage(TypedDict):
    id: str
    filename: str
    download_url: str


class Response(TypedDict):
    dry_run: bool
    queue_url: str
    ingestion_date: str
    count: int
    granules: Sequence[GranuleMessage]


def handler(event: Mapping[str, Any], context: Any) -> Response:
    import boto3
    from db.session import get_session_maker

    print(json.dumps(event))

    queue_url = os.environ["TO_DOWNLOAD_SQS_QUEUE_URL"]
    response = _handler(event, get_session_maker(), boto3.client("sqs"), queue_url)

    print(json.dumps(response))

    return response


def _handler(
    event: Mapping[str, Any],
    make_session: SessionMaker,
    sqs_client: SQSClient,
    queue_url: str,
) -> Response:
    if (dry_run := event["dry_run"]) not in [True, False]:
        raise TypeError("dry_run must be a boolean")

    dt = iso8601.parse_date(event["date"])
    date = datetime.date(dt.year, dt.month, dt.day)
    messages = tuple(map(granule_message, select_missing_granules(date, make_session)))

    if not dry_run:
        for message in messages:
            sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))

    return {
        "dry_run": dry_run,
        "queue_url": queue_url,
        "ingestion_date": f"{date}",
        "count": len(messages),
        "granules": messages,
    }


def select_missing_granules(
    ingestion_date: datetime.date,
    Session: SessionMaker,
) -> Sequence[Granule]:
    conditions = (
        Granule.downloaded == False,  # noqa: E712
        func.date_trunc("day", Granule.ingestiondate) == ingestion_date,
    )

    with Session() as session:
        return session.query(Granule).filter(*conditions).all()  # type: ignore


def granule_message(granule: Granule) -> GranuleMessage:
    return GranuleMessage(
        id=granule.id,
        filename=granule.filename,
        download_url=granule.download_url,
    )
