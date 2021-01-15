import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, TypedDict

import boto3
import humanfriendly
import requests

from db.models.granule import Granule
from db.models.granule_count import GranuleCount
from db.session import get_session

SCIHUB_PRODUCT_URL_FMT = "https://scihub.copernicus.eu/dhus/odata/v1/Products('{}')/"
ACCEPTED_TILE_IDS_FILENAME = "allowed_tiles.txt"


class ScihubResult(TypedDict):
    image_id: str
    filename: str
    tileid: str
    size: int
    checksum: str
    beginposition: datetime
    endposition: datetime
    ingestiondate: datetime
    download_url: str


def handler(event, context):
    accepted_tile_ids = get_accepted_tile_ids()
    for day in get_dates_to_query(last_date=datetime.now()):
        keep_querying_for_imagery = True
        updated_total_results = False
        available_links, fetched_links = get_available_and_fetched_links(day)
        params = get_query_parameters(start=fetched_links, day=day)

        while keep_querying_for_imagery:

            scihub_results, total_results = get_page_for_query_and_total_results(
                query_params=params
            )

            if not updated_total_results:
                update_total_results(total_results)
                updated_total_results = True

            if not scihub_results:
                keep_querying_for_imagery = False
                break

            params["start"] += len(scihub_results)

            filtered_scihub_results = filter_scihub_results(
                scihub_results, accepted_tile_ids
            )

            add_scihub_results_to_db(filtered_scihub_results)


def add_scihub_results_to_db(scihub_results: List[ScihubResult]):
    """
    Creates a record in the `granule` table for each of the provided ScihubResults
    Firstly a check is performed to ensure that a granule isn't already present, if it
    is we don't add it
    :param scihub_results: List[ScihubResult] the list of SciHub results to add to the
        `granule` table
    """
    with get_session() as db:
        for result in scihub_results:
            if not db.query(Granule).filter(Granule.id == result["image_id"]).first():
                db.add(
                    Granule(
                        id=result["image_id"],
                        filename=result["filename"],
                        tileid=result["tileid"],
                        size=result["size"],
                        checksum=result["checksum"],
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


def get_available_and_fetched_links(day: datetime) -> Tuple[int, int]:
    with get_session() as db:
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
        return (granule_count.available_links, granule_count.fetched_links)


def update_total_results(day: datetime, total_results: int):
    with get_session() as db:
        granule_count = db.query(GranuleCount).filter(GranuleCount.date == day).first()
        granule_count.available_links = total_results
        db.commit()


def get_accepted_tile_ids() -> List[str]:
    """
    Returns a list of MGRS square IDs that are acceptable for processing within the
    downloader
    :returns: List[str] representing all of the acceptable MGRS square IDs
    """
    with open(
        os.path.join(
            os.path.basename(os.path.abspath(__file__)), ACCEPTED_TILE_IDS_FILENAME
        ),
        "r",
    ) as tile_ids_in:
        return [line.strip() for line in tile_ids_in]


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


def get_query_parameters(start: int, day: datetime) -> Dict:
    """
    Returns the query parameters that are needed for getting new imagery from
    SciHub/IntHub
    :param start: An int representing the offset to get results from a query
    :param day: A datetime object representing the date to query for imagery
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


def get_dates_to_query(
    last_date: datetime, number_of_dates_to_query: int = 21
) -> List[datetime]:
    """
    Retrieves `number_of_dates_to_query` dates up to and including `last_date`
    Eg. if 2020-01-10 is provided with 3 days to query, datetimes of (2020-01-10,
    2020-01-09, and 2020-01-08) will be returned
    :param last_date: A datetime object representing the last date to query for
    :param number_of_days_to_query: An int representing how many dates to query
    :returns: List[datetime] A list of datetimes representing the queries temporal
        window
    """
    return [
        last_date - timedelta(days=day) for day in range(0, number_of_dates_to_query)
    ]


def get_image_checksum(product_url: str) -> str:
    """
    Returns the string value of a products checksum
    :param product_url: Str representing the Scihub url for the product
    :returns: Str representing the MD5 Checksum of the product
    """
    resp = requests.get(url=f"{product_url}?$format=json&$select=Checksum")
    resp.raise_for_status()

    return resp.json()["d"]["Checksum"]["Value"]


def ensure_three_decimal_points_for_milliseconds_and_replace_z(
    datetimestring: str,
) -> str:
    """
    To convert SciHub Datetimes to Python Datetimes, we need them in ISO format
    SciHub Datetimes can have milliseconds of only two digits, we need three, therefore
    we pad them with a zero to the right
    We also need to replace Z at the end with +00:00
    :param datetimestring: Str representing a SciHub Datetime
    :returns: Str representing a correctly padded SciHub Datetime
    """
    datetimestring_stripped = datetimestring.replace("Z", "")
    if len(datetimestring_stripped.split(".")[1]) < 3:
        datetimestring_stripped = f"{datetimestring_stripped}0"
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

    checksum = get_image_checksum(product_url)

    for date in feed_entry["date"]:
        if date["name"] == "beginposition":
            beginposition = datetime.fromisoformat(
                ensure_three_decimal_points_for_milliseconds_and_replace_z(
                    date["content"]
                )
            )
        elif date["name"] == "endposition":
            endposition = datetime.fromisoformat(
                ensure_three_decimal_points_for_milliseconds_and_replace_z(
                    date["content"]
                )
            )
        elif date["name"] == "ingestiondate":
            ingestiondate = datetime.fromisoformat(
                ensure_three_decimal_points_for_milliseconds_and_replace_z(
                    date["content"]
                )
            )

    download_url = f"{product_url}$value"

    return ScihubResult(
        image_id=image_id,
        filename=filename,
        tileid=tileid,
        size=size,
        checksum=checksum,
        beginposition=beginposition,
        endposition=endposition,
        ingestiondate=ingestiondate,
        download_url=download_url,
    )


def get_page_for_query_and_total_results(
    query_params: Dict,
) -> Tuple[List[ScihubResult], int]:
    """
    Takes a set of query parameters and retrieves the SciHub results that match that
    query. Due to the volume of results, the SciHubResult list returned is a paged
    selection, not the entirety of results matching the query. The number of matching
    results is however returned as well.
    :param query_params: Dict representing the parameters to send to SciHub in a GET
        request for searching imagery
    :returns: Tuple[List[ScihubResult], int] Tuple containing the paged SciHub results
        from the query and an int value representing the total number of results that
        match the query
    """
    resp = requests.get(
        url="https://scihub.copernicus.eu/dhus/search", params=query_params
    )
    resp.raise_for_status()

    query_feed = resp.json()["feed"]
    total_results = int(query_feed["opensearch:totalResults"])

    if "entry" not in query_feed:
        return [], total_results

    scihub_results = [
        create_scihub_result_from_feed_entry(entry) for entry in query_feed["entry"]
    ]

    return scihub_results, total_results
