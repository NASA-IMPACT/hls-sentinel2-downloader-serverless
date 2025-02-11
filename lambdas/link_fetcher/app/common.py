import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Callable,
    Final,
    Sequence,
    Set,
)

import boto3
from db.models.granule import Granule
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from typing_extensions import TypeAlias

if TYPE_CHECKING:
    from mypy_boto3_sqs.client import SQSClient

SessionMaker: TypeAlias = Callable[[], Session]

ACCEPTED_TILE_IDS_FILENAME: Final = "allowed_tiles.txt"


@dataclass(frozen=True)
class SearchResult:
    image_id: str
    filename: str
    tileid: str
    size: int
    beginposition: datetime
    endposition: datetime
    ingestiondate: datetime
    download_url: str
    checksum: str | None = None


def parse_tile_id_from_title(title: str) -> str:
    # The tile ID is encoded into the filename (title).  It is embedded as
    # `_TXXXXX_`, where `XXXXX` is the 5-character alphanumeric tile ID.
    # https://sentinels.copernicus.eu/ca/web/sentinel/user-guides/sentinel-2-msi/naming-convention
    match = re.search("_T(?P<tile_id>[0-9A-Z]{5})_", title)
    tile_id = match["tile_id"] if match else ""
    return tile_id


def get_accepted_tile_ids() -> Set[str]:
    """
    Return MGRS square IDs acceptable for processing within the downloader.

    :returns: set of all acceptable MGRS square IDs
    """
    accepted_tile_ids_filepath = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), ACCEPTED_TILE_IDS_FILENAME
    )

    with open(accepted_tile_ids_filepath) as tile_ids_in:
        return {line.strip() for line in tile_ids_in}


def filter_search_results(
    search_results: Sequence[SearchResult],
    accepted_tile_ids: Set[str],
) -> Sequence[SearchResult]:
    """
    Filters the given search results list and returns a list of results that tile ids
    are within our accepted list of ids.

    :param search_results: List[SearchResult] representing the results of a query to
        search
    :param accepted_tile_ids: Set[str] representing acceptable MGRS tile ids
    :returns: List[searchResult] representing a filtered version of the given results
    """
    return tuple(
        search_result
        for search_result in search_results
        if search_result.tileid in accepted_tile_ids
    )


def add_search_results_to_db_and_sqs(
    session_maker: SessionMaker, search_results: Sequence[SearchResult]
):
    """
    Creates a record in the `granule` table for each of the provided SearchResults and
    a SQS Message in the `To Download` Queue.
    If a record is already in the `granule` table, it will throw an exception which
    when caught, will rollback the insertion and the SQS Message will not be added.
    :param session_maker: sessionmaker representing the SQLAlchemy sessionmaker to use
        for adding results
    :param search_results: list of search results to add to the
        `granule` table
    """
    sqs_client = boto3.client("sqs")
    to_download_queue_url = os.environ["TO_DOWNLOAD_SQS_QUEUE_URL"]

    with session_maker() as session:
        for result in search_results:
            try:
                session.add(
                    Granule(
                        id=result.image_id,
                        filename=result.filename,
                        tileid=result.tileid,
                        size=result.size,
                        beginposition=result.beginposition,
                        endposition=result.endposition,
                        ingestiondate=result.ingestiondate,
                        download_url=result.download_url,
                        checksum=result.checksum or "",
                    )  # type: ignore
                )
                session.commit()
                add_search_result_to_sqs(result, sqs_client, to_download_queue_url)
            except IntegrityError:
                print(f"{result.image_id} already in Database, not adding")
                session.rollback()


def add_search_result_to_sqs(
    search_result: SearchResult, sqs_client: "SQSClient", queue_url: str
):
    """
    Creates a message in the provided SQS queue for the provided
    SearchResult. The message is in the form {"id": <val>, "download_url": <val>}
    :param search_result: search result to add to the SQS queue
    :param sqs_client: SQSClient representing a boto3 SQS client
    :param queue_url: str presenting the URL of the queue to send the message to
    """
    message = {
        "id": search_result.image_id,
        "filename": search_result.filename,
        "download_url": search_result.download_url,
    }
    if search_result.checksum is not None:
        message["checksum"] = search_result.checksum

    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message),
    )
