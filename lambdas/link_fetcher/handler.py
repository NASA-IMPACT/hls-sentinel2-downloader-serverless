import json
import os
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Tuple

import boto3
import humanfriendly
import requests
from db.models.granule import Granule
from db.models.granule_count import GranuleCount
from db.models.status import Status
from db.session import get_session, get_session_maker

from scihub_result import ScihubResult

SCIHUB_PRODUCT_URL_FMT = "https://scihub.copernicus.eu/dhus/odata/v1/Products('{}')/"
ACCEPTED_TILE_IDS_FILENAME = "allowed_tiles.txt"


def handler(event, context):
    session_maker = get_session_maker()
    accepted_tile_ids = get_accepted_tile_ids()
    scihub_auth = get_scihub_auth()
    keep_querying_for_imagery = True
    updated_total_results = False
    day = datetime.strptime(event["query_date"], "%Y-%m-%d").date()

    available_links, fetched_links = get_available_and_fetched_links(
        session_maker, day
    )
    params = get_query_parameters(fetched_links, day)

    while keep_querying_for_imagery:
        # ts = time.time()
        scihub_results, total_results = get_page_for_query_and_total_results(
            params, scihub_auth
        )
        # te = time.time()
        # print(f"{te - ts}s for getting page")

        if not updated_total_results:
            update_total_results(session_maker, day, total_results)
            updated_total_results = True

        if not scihub_results:
            keep_querying_for_imagery = False
            break

        number_of_fetched_links = len(scihub_results)
        params["start"] += number_of_fetched_links

        # ts = time.time()
        filtered_scihub_results = filter_scihub_results(
            scihub_results, accepted_tile_ids
        )
        # te = time.time()
        # print(f"{te - ts}s for filtering results")

        # ts = time.time()
        add_scihub_results_to_db(session_maker, filtered_scihub_results)
        # te = time.time()
        # print(f"{te - ts}s for adding results to db")
        # ts = time.time()
        add_scihub_results_to_sqs(filtered_scihub_results)
        # te = time.time()
        # print(f"{te - ts}s for adding results to sqs")

        update_last_fetched_link_time(session_maker)
        update_fetched_links(session_maker, day, number_of_fetched_links)


def add_scihub_results_to_db(session_maker, scihub_results: List[ScihubResult]):
    """
    Creates a record in the `granule` table for each of the provided ScihubResults
    Firstly a check is performed to ensure that a granule isn't already present, if it
    is we don't add it
    :param scihub_results: List[ScihubResult] the list of SciHub results to add to the
        `granule` table
    """
    with get_session(session_maker) as db:
        for result in scihub_results:
            if not db.query(Granule).filter(Granule.id == result["image_id"]).first():
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


def add_scihub_results_to_sqs(scihub_results: List[ScihubResult]):
    """
    Creates messages in the `To Download` SQS queue for each of the provided
    ScihubResults. The message is in the form {"id": <val>, "download_url": <val>}
    :param scihub_results: List[ScihubResult] the list of SciHub results to add to the
        `To Download` SQS queue
    """
    sqs_client = boto3.client("sqs")
    to_download_queue_url = os.environ["TO_DOWNLOAD_SQS_QUEUE_URL"]
    for result in scihub_results:
        sqs_client.send_message(
            QueueUrl=to_download_queue_url,
            MessageBody=json.dumps(
                {"id": result["image_id"], "download_url": result["download_url"]}
            ),
        )


def get_available_and_fetched_links(session_maker, day: date) -> Tuple[int, int]:
    """
    For a given day, return the values for total `available_links` and total
    `fetched_links`, where `available_links` is the total number of results for the day
    from SciHub and `fetched_links` is the total number of granules that have been
    processed (but not necessarily added to the database because of filtering)

    If no entry is found, one is created
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


def update_total_results(session_maker, day: date, total_results: int):
    """
    For a given day and number of results, update the `available_links` value
    :param day: date representing the day to update `available_links` for
    :param total_results: int representing the total results available for the day,
        this value will be applied to `available_links`
    """
    with get_session(session_maker) as db:
        granule_count = db.query(GranuleCount).filter(GranuleCount.date == day).first()
        granule_count.available_links = total_results
        db.commit()


def update_last_fetched_link_time(session_maker):
    """
    Update the `last_linked_fetched_time` value in the `status` table
    Will set the value to `datetime.now()`, if not already present, the value will be
    created
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


def update_fetched_links(session_maker, day: date, fetched_links: int):
    """
    For a given day, update the `fetched_links` value in `granule_count` to the provided
    `fetched_links` value and update the `last_fetched_time` value to `datetime.now()`
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


# def get_dates_to_query(
#     last_date: date, number_of_dates_to_query: int = 21
# ) -> List[date]:
#     """
#     Retrieves `number_of_dates_to_query` dates up to and including `last_date`
#     Eg. if 2020-01-10 is provided with 3 days to query, datetimes of (2020-01-10,
#     2020-01-09, and 2020-01-08) will be returned
#     :param last_date: A date object representing the last date to query for
#     :param number_of_days_to_query: An int representing how many dates to query
#     :returns: List[date] A list of dates representing the queries temporal
#         window
#     """
#     return [
#         last_date - timedelta(days=day) for day in range(0, number_of_dates_to_query)
#     ]


# def get_image_checksum(product_url: str, auth: Tuple[str, str]) -> str:
#     """
#     Returns the string value of a products checksum
#     :param product_url: Str representing the Scihub url for the product
#     :param auth: Tuple[str, str] representing the username and password for SciHub
#     :returns: Str representing the MD5 Checksum of the product
#     """
#     resp = requests.get(url=f"{product_url}?$format=json&$select=Checksum", auth=auth)
#     resp.raise_for_status()

#     return resp.json()["d"]["Checksum"]["Value"]


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


def create_scihub_result_from_feed_entry(
    feed_entry: Dict, auth: Tuple[str, str]
) -> ScihubResult:
    """
    Creates a SciHubResult object from a feed entry returned from a SciHub query
    :param feed_entry: A Dict representing the feed entry for one image
    :param auth: Tuple[str, str] representing the username and password for SciHub
    :returns: SciHubResult A object with information useful for the Downloader
    """
    image_id = feed_entry["id"]
    product_url = SCIHUB_PRODUCT_URL_FMT.format(image_id)

    # ts = time.time()
    for string in feed_entry["str"]:
        if string["name"] == "filename":
            filename = string["content"]
        elif string["name"] == "tileid":
            tileid = string["content"]
        elif string["name"] == "size":
            size = humanfriendly.parse_size(string["content"], binary=True)
    # te = time.time()
    # print(f"{te - ts}s to get string entries")

    # ts = time.time()
    # checksum = get_image_checksum(product_url, auth)
    # te = time.time()
    # print(f"{te - ts}s to get checksum")

    # ts = time.time()
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
    # te = time.time()
    # print(f"{te - ts}s to get date entries")

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
    # ts = time.time()
    resp = requests.get(
        url="https://scihub.copernicus.eu/dhus/search", params=query_params, auth=auth
    )
    resp.raise_for_status()
    query_feed = resp.json()["feed"]
    total_results = int(query_feed["opensearch:totalResults"])
    # te = time.time()
    # print(f"{te - ts}s to get query")

    if "entry" not in query_feed:
        return [], total_results

    # ts = time.time()
    scihub_results = [
        create_scihub_result_from_feed_entry(entry, auth)
        for entry in query_feed["entry"]
    ]
    # te = time.time()
    # print(f"{te - ts}s to create scihub results")

    return scihub_results, total_results
