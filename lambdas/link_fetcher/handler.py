import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Final,
    Mapping,
    Protocol,
    Sequence,
    Set,
    Tuple,
    TypedDict,
)

import backoff
import boto3
import humanfriendly
import iso8601
import requests
from db.models.granule import Granule
from db.models.granule_count import GranuleCount
from db.models.status import Status
from db.session import get_session_maker
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from typing_extensions import TypeAlias

if TYPE_CHECKING:
    from mypy_boto3_sqs.client import SQSClient

SessionMaker: TypeAlias = Callable[[], Session]

ACCEPTED_TILE_IDS_FILENAME: Final = "allowed_tiles.txt"
MIN_REMAINING_MILLIS: Final = 60_000
SEARCH_URL: Final = (
    "https://catalogue.dataspace.copernicus.eu/resto/api/collections"
    "/Sentinel2/search.json"
)

# Log `backoff` library's retry attempts on request failures
logging.getLogger("backoff").addHandler(logging.StreamHandler())


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


class Context(Protocol):
    def get_remaining_time_in_millis(self) -> int:
        ...


class HandlerResult(TypedDict):
    query_date: str
    completed: bool


def handler(event: Mapping[str, Any], context: Context) -> HandlerResult:
    return _handler(event, context, get_session_maker())


def _handler(
    event: Mapping[str, Any],
    context: Context,
    session_maker: SessionMaker,
) -> HandlerResult:
    accepted_tile_ids = get_accepted_tile_ids()
    query_date = event["query_date"]
    day = datetime.strptime(query_date, "%Y-%m-%d").date()

    fetched_links = get_fetched_links(session_maker, day)
    params = get_query_parameters(fetched_links, day)
    search_results, total_results = get_page_for_query_and_total_results(params)
    print(f"Previously fetched links for {query_date}: {fetched_links}/{total_results}")
    update_total_results(session_maker, day, total_results)
    bail_early = False

    while search_results:
        number_of_fetched_links = len(search_results)
        filtered_search_results = filter_search_results(
            search_results, accepted_tile_ids
        )
        add_search_results_to_db_and_sqs(session_maker, filtered_search_results)
        update_last_fetched_link_time(session_maker)
        update_fetched_links(session_maker, day, number_of_fetched_links)

        params = {**params, "index": params["index"] + number_of_fetched_links}
        print(f"Fetched links for {query_date}: {params['index'] - 1}/{total_results}")

        if bail_early := context.get_remaining_time_in_millis() < MIN_REMAINING_MILLIS:
            print("Bailing early to avoid Lambda timeout")
            break

        search_results, _ = get_page_for_query_and_total_results(params)

    return {"query_date": query_date, "completed": not bail_early}


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
    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(
            {
                "id": search_result.image_id,
                "filename": search_result.filename,
                "download_url": search_result.download_url,
            }
        ),
    )


def get_fetched_links(session_maker: SessionMaker, day: date) -> int:
    """
    For a given day, return the total
    `fetched_links`, where `fetched_links` is the total number of granules that have
    been processed (but not necessarily added to the database because of filtering)

    If no entry is found, one is created
    :param session_maker: sessionmaker representing the SQLAlchemy sessionmaker to use
        for database interactions
    :param day: date representing the day to return results for
    :returns: int representing `fetched_links`
    """
    with session_maker() as session:
        granule_count = session.query(GranuleCount).filter_by(date=day).first()

        if granule_count:
            return granule_count.fetched_links

        granule_count = GranuleCount(
            date=day,
            available_links=0,
            fetched_links=0,
            last_fetched_time=datetime.now(),
        )  # type: ignore
        session.add(granule_count)
        session.commit()

        return 0


def update_total_results(session_maker: SessionMaker, day: date, total_results: int):
    """
    For a given day and number of results, update the `available_links` value
    :param session_maker: sessionmaker representing the SQLAlchemy sessionmaker to use
        for database interactions
    :param day: date representing the day to update `available_links` for
    :param total_results: int representing the total results available for the day,
        this value will be applied to `available_links`
    """
    with session_maker() as session:
        if granule_count := session.query(GranuleCount).filter_by(date=day).first():
            granule_count.available_links = total_results
            session.commit()


def update_last_fetched_link_time(session_maker: SessionMaker):
    """
    Update the `last_linked_fetched_time` value in the `status` table
    Will set the value to `datetime.now()`, if not already present, the value will be
    created
    :param session_maker: sessionmaker representing the SQLAlchemy sessionmaker to use
        for database interactions
    """
    last_fetched_key_name = "last_linked_fetched_time"
    datetime_now = str(datetime.now())

    with session_maker() as session:
        if last_linked_fetched_time := (
            session.query(Status).filter_by(key_name=last_fetched_key_name).first()
        ):
            last_linked_fetched_time.value = datetime_now
        else:
            session.add(Status(key_name=last_fetched_key_name, value=datetime_now))  # type: ignore

        session.commit()


def update_fetched_links(session_maker: SessionMaker, day: date, fetched_links: int):
    """
    For a given day, update the `fetched_links` value in `granule_count` to the provided
    `fetched_links` value and update the `last_fetched_time` value to `datetime.now()`
    :param session_maker: sessionmaker representing the SQLAlchemy sessionmaker to use
        for database interactions
    :param day: date representing the day to update in `granule_count`
    :param fetched_links: int representing the total number of links fetched in this run
        it is not the total number of Granules created
    """
    with session_maker() as session:
        if granule_count := session.query(GranuleCount).filter_by(date=day).first():
            granule_count.fetched_links += fetched_links
            granule_count.last_fetched_time = datetime.now()
            session.commit()


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


def get_query_parameters(start: int, day: date) -> Mapping[str, Any]:
    """
    Returns the query parameters that are needed for getting new imagery from
    search/IntHub
    :param start: An int representing the offset to get results from a query
    :param day: A date object representing the date to query for imagery
    :returns: mapping of query parameters
    """
    date_string = day.strftime("%Y-%m-%d")
    oldest_acquisition_date = day - timedelta(days=30)

    return {
        "processingLevel": "S2MSI1C",
        "publishedAfter": f"{date_string}T00:00:00Z",
        "publishedBefore": f"{date_string}T23:59:59Z",
        "startDate": f"{oldest_acquisition_date.strftime('%Y-%m-%d')}T00:00:00Z",
        "sortParam": "published",
        "sortOrder": "desc",
        "maxRecords": 100,
        # `start` is 0-based, but `index` is 1-based, so we must add 1
        "index": start + 1,
    }


def create_search_result(search_item: Mapping[str, Any]) -> SearchResult:
    """
    Create a SearchResult object from an untyped item from a search query.

    :param search_item: untyped item for one image
    :returns: search result with information useful for the Downloader
    """
    properties = search_item["properties"]
    download = properties["services"]["download"]
    size = humanfriendly.parse_size(str(download["size"]), binary=True)
    title = properties["title"]

    # The tile ID is encoded into the filename (title).  It is embedded as
    # `_TXXXXX_`, where `XXXXX` is the 5-character alphanumeric tile ID.
    # https://sentinels.copernicus.eu/ca/web/sentinel/user-guides/sentinel-2-msi/naming-convention
    match = re.search("_T(?P<tile_id>[0-9A-Z]{5})_", title)
    tile_id = match["tile_id"] if match else ""

    return SearchResult(
        image_id=search_item["id"],
        filename=title,
        tileid=tile_id,
        size=size,
        beginposition=iso8601.parse_date(properties["startDate"]),
        endposition=iso8601.parse_date(properties["completionDate"]),
        ingestiondate=iso8601.parse_date(properties["published"]),
        download_url=download["url"],
    )


def client_error(e: Exception) -> bool:
    return (
        # `e` will always be a RequestException, but the type signature required
        # for this function is `Exception`, so we're simply making the type
        # checker happy, so it doesn't flag `response` as an unknown member.
        isinstance(e, requests.RequestException)
        and hasattr(e, "response")
        and e.response.status_code is not None
        and 400 <= e.response.status_code < 500
    )


@backoff.on_exception(
    backoff.expo,
    requests.RequestException,
    max_tries=20,  # Be somewhat persistent in the face of 503 responses
    max_time=10 * 60,  # Max time between retries: 10 minutes (measured in seconds)
    giveup=client_error,  # Don't retry 4XX responses
)
def get_page_for_query_and_total_results(
    query_params: Mapping[str, Any]
) -> Tuple[Sequence[SearchResult], int]:
    """
    Takes a set of query parameters and retrieves the search results that match that
    query. Due to the volume of results, the search results list returned is a paged
    selection, not the entirety of results matching the query. The number of matching
    results is however returned as well.

    :param query_params: query parameters to use in a GET request for searching imagery
    :returns: tuple containing the paged search results from the query and the total
        number of results that match the query
    """

    resp = requests.get(SEARCH_URL, params=query_params)
    resp.raise_for_status()
    results = resp.json()
    total_results = results["properties"]["totalResults"]

    search_results = tuple(
        create_search_result(entry) for entry in results.get("features", [])
    )

    return search_results, total_results
