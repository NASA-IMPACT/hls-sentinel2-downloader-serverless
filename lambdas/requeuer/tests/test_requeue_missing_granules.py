import json
from datetime import date, datetime

import pytest
from db.models.granule import Granule
from handler import Response, _handler
from mypy_boto3_sqs.client import SQSClient
from mypy_boto3_sqs.service_resource import Queue
from sqlalchemy.orm import Session  # type: ignore


def test_missing_dry_run_raises(
    db_session: Session,
    sqs_client: SQSClient,
    sqs_queue: Queue,
):
    with pytest.raises(KeyError, match="dry_run"):
        _handler(
            dict(date="2021-01-01"),
            lambda: db_session,
            sqs_client,
            sqs_queue.url,
        )


def test_missing_date_raises(
    db_session: Session,
    sqs_client: SQSClient,
    sqs_queue: Queue,
):
    with pytest.raises(KeyError, match="date"):
        _handler(
            dict(dry_run=False),
            lambda: db_session,
            sqs_client,
            sqs_queue.url,
        )


def test_invalid_dry_run_raises(
    db_session: Session,
    sqs_client: SQSClient,
    sqs_queue: Queue,
):
    with pytest.raises(TypeError, match="dry_run"):
        _handler(
            dict(dry_run="foo", date="2021-01-01"),
            lambda: db_session,
            sqs_client,
            sqs_queue.url,
        )


def test_invalid_date_raises(
    db_session: Session,
    sqs_client: SQSClient,
    sqs_queue: Queue,
):
    with pytest.raises(ValueError, match="date"):
        _handler(
            dict(dry_run=False, date="foo"),
            lambda: db_session,
            sqs_client,
            sqs_queue.url,
        )


def test_dry_run_doesnt_enqueue(
    db_session: Session,
    sqs_client: SQSClient,
    sqs_queue: Queue,
):
    ingestion_mdy = (2021, 1, 1)
    ingestion_datetime = datetime(*ingestion_mdy, 12, 0, 0)
    ingestion_date = date(*ingestion_mdy)

    db_session.add(
        Granule(
            id="foo",
            filename="foo.tif",
            tileid="foo",
            size=100,
            beginposition=ingestion_datetime,
            endposition=ingestion_datetime,
            ingestiondate=ingestion_datetime,
            download_url="https://example.com/foo.tif",
            downloaded=False,
        ),  # type: ignore
    )
    db_session.add(
        Granule(
            id="bar",
            filename="bar.tif",
            tileid="bar",
            size=100,
            beginposition=ingestion_datetime,
            endposition=ingestion_datetime,
            ingestiondate=ingestion_datetime,
            download_url="https://example.com/bar.tif",
            downloaded=False,
        ),  # type: ignore
    )
    db_session.commit()

    actual = _handler(
        dict(dry_run=True, date=f"{ingestion_date}"),
        lambda: db_session,
        sqs_client,
        sqs_queue.url,
    )
    expected = dict(
        dry_run=True,
        queue_url=sqs_queue.url,
        ingestion_date="2021-01-01",
        count=2,
        granules=(
            {
                "id": "foo",
                "filename": "foo.tif",
                "download_url": "https://example.com/foo.tif",
            },
            {
                "id": "bar",
                "filename": "bar.tif",
                "download_url": "https://example.com/bar.tif",
            },
        ),
    )
    received = sqs_client.receive_message(
        QueueUrl=sqs_queue.url, MaxNumberOfMessages=10
    )

    assert actual == expected
    assert "Messages" not in received


def test_none_missing_for_date(
    db_session: Session,
    sqs_client: SQSClient,
    sqs_queue: Queue,
):
    actual = _handler(
        dict(dry_run=False, date="2021-01-01"),
        lambda: db_session,
        sqs_client,
        sqs_queue.url,
    )
    expected = dict(
        dry_run=False,
        queue_url=sqs_queue.url,
        ingestion_date="2021-01-01",
        count=0,
        granules=(),
    )
    received = sqs_client.receive_message(
        QueueUrl=sqs_queue.url, MaxNumberOfMessages=10
    )

    assert actual == expected
    assert "Messages" not in received


def test_some_missing_for_date(
    db_session: Session,
    sqs_client: SQSClient,
    sqs_queue: Queue,
):
    ingestion_mdy = (2021, 1, 1)
    ingestion_datetime = datetime(*ingestion_mdy, 12, 0, 0)
    ingestion_date = date(*ingestion_mdy)

    db_session.add(
        Granule(
            id="foo",
            filename="foo.tif",
            tileid="foo",
            size=100,
            beginposition=ingestion_datetime,
            endposition=ingestion_datetime,
            ingestiondate=ingestion_datetime,
            download_url="https://example.com/foo.tif",
            downloaded=False,
        ),  # type: ignore
    )
    db_session.add(
        Granule(
            id="bar",
            filename="bar.tif",
            tileid="bar",
            size=100,
            beginposition=ingestion_datetime,
            endposition=ingestion_datetime,
            ingestiondate=ingestion_datetime,
            download_url="https://example.com/bar.tif",
            downloaded=True,
        ),  # type: ignore
    )
    db_session.commit()

    actual = _handler(
        dict(dry_run=False, date=f"{ingestion_date}"),
        lambda: db_session,
        sqs_client,
        sqs_queue.url,
    )
    expected = Response(
        dry_run=False,
        queue_url=sqs_queue.url,
        ingestion_date=f"{ingestion_date}",
        count=1,
        granules=(
            {
                "id": "foo",
                "filename": "foo.tif",
                "download_url": "https://example.com/foo.tif",
            },
        ),
    )
    received = sqs_client.receive_message(
        QueueUrl=sqs_queue.url, MaxNumberOfMessages=10
    )

    assert actual == expected
    assert len(received["Messages"]) == 1
    assert received["Messages"][0].get("Body") == json.dumps(expected["granules"][0])


def test_all_missing_for_date(
    db_session: Session,
    sqs_client: SQSClient,
    sqs_queue: Queue,
):
    ingestion_mdy = (2021, 1, 1)
    ingestion_datetime = datetime(*ingestion_mdy, 12, 0, 0)
    ingestion_date = date(*ingestion_mdy)

    db_session.add(
        Granule(
            id="foo",
            filename="foo.tif",
            tileid="foo",
            size=100,
            beginposition=ingestion_datetime,
            endposition=ingestion_datetime,
            ingestiondate=ingestion_datetime,
            download_url="https://example.com/foo.tif",
            downloaded=False,
        ),  # type: ignore
    )
    db_session.add(
        Granule(
            id="bar",
            filename="bar.tif",
            tileid="bar",
            size=100,
            beginposition=ingestion_datetime,
            endposition=ingestion_datetime,
            ingestiondate=ingestion_datetime,
            download_url="https://example.com/bar.tif",
            downloaded=False,
        ),  # type: ignore
    )
    db_session.commit()

    actual = _handler(
        dict(dry_run=False, date=f"{ingestion_date}"),
        lambda: db_session,
        sqs_client,
        sqs_queue.url,
    )
    expected = Response(
        dry_run=False,
        queue_url=sqs_queue.url,
        ingestion_date=f"{ingestion_date}",
        count=2,
        granules=(
            {
                "id": "foo",
                "filename": "foo.tif",
                "download_url": "https://example.com/foo.tif",
            },
            {
                "id": "bar",
                "filename": "bar.tif",
                "download_url": "https://example.com/bar.tif",
            },
        ),
    )
    received = sqs_client.receive_message(
        QueueUrl=sqs_queue.url, MaxNumberOfMessages=10
    )

    assert actual == expected
    assert len(received["Messages"]) == 2
    assert received["Messages"][0].get("Body") == json.dumps(expected["granules"][0])
    assert received["Messages"][1].get("Body") == json.dumps(expected["granules"][1])
