import json
import os
from datetime import date, datetime
from typing import Dict, List, Tuple

import boto3
import humanfriendly
import requests
from db.models.granule import Granule
from db.models.granule_count import GranuleCount
from db.models.status import Status
from db.session import get_session, get_session_maker
from mypy_boto3_sqs.client import SQSClient
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from scihub_result import ScihubResult

SCIHUB_URL = os.environ.get("SCIHUB_URL", "https://scihub.copernicus.eu")
SCIHUB_PRODUCT_URL_FMT = f"{SCIHUB_URL}/dhus/odata/v1/Products('{{}}')/"
ACCEPTED_TILE_IDS_FILENAME = "allowed_tiles.txt"


def handler(event, context):
    session_maker = get_session_maker()
    accepted_tile_ids = get_accepted_tile_ids()
    scihub_auth = get_scihub_auth()
    keep_querying_for_imagery = True
    updated_total_results = False
    day = datetime.strptime(event["query_date"], "%Y-%m-%d").date()

    available_links, fetched_links = get_available_and_fetched_links(session_maker, day)
    params = get_query_parameters(fetched_links, day)

    while keep_querying_for_imagery:

        scihub_results, total_results = get_page_for_query_and_total_results(
            params, scihub_auth
        )

        if not updated_total_results:
            update_total_results(session_maker, day, total_results)
            updated_total_results = True

        if not scihub_results:
            keep_querying_for_imagery = False
            break

        number_of_fetched_links = len(scihub_results)
        params["start"] += number_of_fetched_links

        filtered_scihub_results = filter_scihub_results(
            scihub_results, accepted_tile_ids
        )

        add_scihub_results_to_db_and_sqs(session_maker, filtered_scihub_results)

        update_last_fetched_link_time(session_maker)
        update_fetched_links(session_maker, day, number_of_fetched_links)


def add_scihub_results_to_db_and_sqs(
    session_maker: sessionmaker, scihub_results: List[ScihubResult]
):
    """
    Creates a record in the `granule` table for each of the provided ScihubResults and
    a SQS Message in the `To Download` Queue.
    If a record is already in the `granule` table, it will throw an exception which
    when caught, will rollback the insertion and the SQS Message will not be added.
    :param session_maker: sessionmaker representing the SQLAlchemy sessionmaker to use
        for adding results
    :param scihub_results: List[ScihubResult] the list of SciHub results to add to the
        `granule` table
    """
    sqs_client = boto3.client("sqs")
    to_download_queue_url = os.environ["TO_DOWNLOAD_SQS_QUEUE_URL"]
    for result in scihub_results:
        with get_session(session_maker) as db:
            try:
                db.add(
                    Granule(
                        id=result["image_id"],
                        filename=result["filename"],
                        tileid=result["tileid"],
                        size=result["size"],
                        beginposition=result["beginposition"],
                        endposition=result["endposition"],
                        ingestiondate=result["ingestiondate"],
                        download_url=result["download_url"],
                    )
                )
                db.commit()
                add_scihub_result_to_sqs(result, sqs_client, to_download_queue_url)
            except IntegrityError:
                print(f"{result['image_id']} already in Database, not adding")
                db.rollback()


def add_scihub_result_to_sqs(
    scihub_result: ScihubResult, sqs_client: SQSClient, queue_url: str
):
    """
    Creates a message in the provided SQS queue for the provided
    ScihubResult. The message is in the form {"id": <val>, "download_url": <val>}
    :param scihub_result: ScihubResult the SciHub result to add to the SQS queue
    :param sqs_client: SQSClient representing a boto3 SQS client
    :param queue_url: str presenting the URL of the queue to send the message to
    """
    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(
            {
                "id": scihub_result["image_id"],
                "download_url": scihub_result["download_url"],
            }
        ),
    )


def get_available_and_fetched_links(
    session_maker: sessionmaker, day: date
) -> Tuple[int, int]:
    """
    For a given day, return the values for total `available_links` and total
    `fetched_links`, where `available_links` is the total number of results for the day
    from SciHub and `fetched_links` is the total number of granules that have been
    processed (but not necessarily added to the database because of filtering)

    If no entry is found, one is created
    :param session_maker: sessionmaker representing the SQLAlchemy sessionmaker to use
        for database interactions
    :param day: date representing the day to return results for
    :returns: Tuple[int, int] representing a tuple of
        (`available_links`, `fetched_links)
    """
    with get_session(session_maker) as db:
        granule_count = db.query(GranuleCount).filter(GranuleCount.date == day).first()
        if not granule_count:
            granule_count = GranuleCount(
                date=day,
                available_links=0,
                fetched_links=0,
                last_fetched_time=datetime.now(),
            )
            db.add(granule_count)
            db.commit()
            return (0, 0)
        else:
            return (granule_count.available_links, granule_count.fetched_links)


def update_total_results(session_maker: sessionmaker, day: date, total_results: int):
    """
    For a given day and number of results, update the `available_links` value
    :param session_maker: sessionmaker representing the SQLAlchemy sessionmaker to use
        for database interactions
    :param day: date representing the day to update `available_links` for
    :param total_results: int representing the total results available for the day,
        this value will be applied to `available_links`
    """
    with get_session(session_maker) as db:
        granule_count = db.query(GranuleCount).filter(GranuleCount.date == day).first()
        granule_count.available_links = total_results
        db.commit()


def update_last_fetched_link_time(session_maker: sessionmaker):
    """
    Update the `last_linked_fetched_time` value in the `status` table
    Will set the value to `datetime.now()`, if not already present, the value will be
    created
    :param session_maker: sessionmaker representing the SQLAlchemy sessionmaker to use
        for database interactions
    """
    last_fetched_key_name = "last_linked_fetched_time"
    datetime_now = str(datetime.now())
    with get_session(session_maker) as db:
        last_linked_fetched_time = (
            db.query(Status).filter(Status.key_name == last_fetched_key_name).first()
        )
        if not last_linked_fetched_time:
            db.add(Status(key_name=last_fetched_key_name, value=datetime_now))
            db.commit()
        else:
            last_linked_fetched_time.value = datetime_now
            db.commit()


def update_fetched_links(session_maker: sessionmaker, day: date, fetched_links: int):
    """
    For a given day, update the `fetched_links` value in `granule_count` to the provided
    `fetched_links` value and update the `last_fetched_time` value to `datetime.now()`
    :param session_maker: sessionmaker representing the SQLAlchemy sessionmaker to use
        for database interactions
    :param day: date representing the day to update in `granule_count`
    :param fetched_links: int representing the total number of links fetched in this run
        it is not the total number of Granules created
    """
    with get_session(session_maker) as db:
        granule_count = db.query(GranuleCount).filter(GranuleCount.date == day).first()
        granule_count.fetched_links += fetched_links
        granule_count.last_fetched_time = datetime.now()
        db.commit()


def get_accepted_tile_ids() -> List[str]:
    """
    Returns a list of MGRS square IDs that are acceptable for processing within the
    downloader
    :returns: List[str] representing all of the acceptable MGRS square IDs
    """
    with open(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)), ACCEPTED_TILE_IDS_FILENAME
        ),
        "r",
    ) as tile_ids_in:
        return [line.strip() for line in tile_ids_in]


def get_scihub_auth() -> Tuple[str, str]:
    """
    Retrieves the username and password for SciHub which is stored in SecretsManager
    :returns: Tuple[str, str] representing the SciHub username and password
    """
    stage = os.environ["STAGE"]
    secrets_manager_client = boto3.client("secretsmanager")
    secret = json.loads(
        secrets_manager_client.get_secret_value(
            SecretId=f"hls-s2-downloader-serverless/{stage}/scihub-credentials"
        )["SecretString"]
    )
    return (secret["username"], secret["password"])


def filter_scihub_results(
    scihub_results: List[ScihubResult], accepted_tile_ids: List[str]
) -> List[ScihubResult]:
    """
    Filters the given SciHub results list and returns a list of results that tile ids
    are within our accepted list of ids
    :param scihub_results: List[ScihubResult] representing the results of a query to
        SciHub
    :param accepted_tile_ids: List[str] representing a list of acceptable MGRS tile ids
    :returns: List[SciHubResult] representing a filtered version of the passed in list
    """
    return [
        scihub_result
        for scihub_result in scihub_results
        if scihub_result["tileid"] in accepted_tile_ids
    ]


def get_query_parameters(start: int, day: date) -> Dict:
    """
    Returns the query parameters that are needed for getting new imagery from
    SciHub/IntHub
    :param start: An int representing the offset to get results from a query
    :param day: A date object representing the date to query for imagery
    :returns: Dict representing the query parameters
    """
    date_string = day.strftime("%Y-%m-%d")
    return {
        "q": (
            "(platformname:Sentinel-2) AND (processinglevel:Level-1C) AND "
            f"(ingestiondate:[{date_string}T00:00:00Z TO {date_string}T23:59:59Z])"
        ),
        "rows": 100,
        "format": "json",
        "orderby": "ingestiondate desc",
        "start": start,
    }


def ensure_three_decimal_points_for_milliseconds_and_replace_z(
    datetimestring: str,
) -> str:
    """
    To convert SciHub Datetimes to Python Datetimes, we need them in ISO format
    SciHub Datetimes can have milliseconds of less than 3 digits therefore
    we pad them with zeros to the right to make 3 digits, as required by `datetime`
    We also need to replace Z at the end with +00:00
    :param datetimestring: Str representing a SciHub Datetime
    :returns: Str representing a correctly padded SciHub Datetime
    """
    datetimestring_stripped = datetimestring.replace("Z", "")
    try:
        number_of_decimal_points = len(datetimestring_stripped.split(".")[1])
        if number_of_decimal_points < 3:
            datetimestring_stripped = (
                f"{datetimestring_stripped}{(3 - number_of_decimal_points) * '0'}"
            )
    except IndexError:
        datetimestring_stripped = f"{datetimestring_stripped}.000"
    return f"{datetimestring_stripped}+00:00"


def create_scihub_result_from_feed_entry(feed_entry: Dict) -> ScihubResult:
    """
    Creates a SciHubResult object from a feed entry returned from a SciHub query
    :param feed_entry: A Dict representing the feed entry for one image
    :returns: SciHubResult A object with information useful for the Downloader
    """
    image_id = feed_entry["id"]
    product_url = SCIHUB_PRODUCT_URL_FMT.format(image_id)

    for string in feed_entry["str"]:
        if string["name"] == "filename":
            filename = string["content"]
        elif string["name"] == "tileid":
            tileid = string["content"]
        elif string["name"] == "size":
            size = humanfriendly.parse_size(string["content"], binary=True)

    for date_entry in feed_entry["date"]:
        if date_entry["name"] == "beginposition":
            beginposition = datetime.fromisoformat(
                ensure_three_decimal_points_for_milliseconds_and_replace_z(
                    date_entry["content"]
                )
            )
        elif date_entry["name"] == "endposition":
            endposition = datetime.fromisoformat(
                ensure_three_decimal_points_for_milliseconds_and_replace_z(
                    date_entry["content"]
                )
            )
        elif date_entry["name"] == "ingestiondate":
            ingestiondate = datetime.fromisoformat(
                ensure_three_decimal_points_for_milliseconds_and_replace_z(
                    date_entry["content"]
                )
            )

    download_url = f"{product_url}$value"

    return ScihubResult(
        image_id=image_id,
        filename=filename,
        tileid=tileid,
        size=size,
        beginposition=beginposition,
        endposition=endposition,
        ingestiondate=ingestiondate,
        download_url=download_url,
    )


def get_page_for_query_and_total_results(
    query_params: Dict, auth: Tuple[str, str]
) -> Tuple[List[ScihubResult], int]:
    """
    Takes a set of query parameters and retrieves the SciHub results that match that
    query. Due to the volume of results, the SciHubResult list returned is a paged
    selection, not the entirety of results matching the query. The number of matching
    results is however returned as well.
    :param query_params: Dict representing the parameters to send to SciHub in a GET
        request for searching imagery
    :param auth: Tuple[str, str] representing the username and password for SciHub
    :returns: Tuple[List[ScihubResult], int] Tuple containing the paged SciHub results
        from the query and an int value representing the total number of results that
        match the query
    """

    resp = requests.get(url=f"{SCIHUB_URL}/dhus/search", params=query_params, auth=auth)
    resp.raise_for_status()
    query_feed = resp.json()["feed"]
    total_results = int(query_feed["opensearch:totalResults"])

    if "entry" not in query_feed:
        return [], total_results

    scihub_results = [
        create_scihub_result_from_feed_entry(entry)
        for entry in query_feed["entry"]
    ]

    return scihub_results, total_results
