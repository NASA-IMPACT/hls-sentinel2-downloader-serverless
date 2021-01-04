import os
from datetime import datetime, timedelta
from typing import Dict, List, TypedDict

import requests


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
    for day in get_dates_to_query(last_date=datetime.now()):
        keep_querying_for_imagery = True
        available_links, fetched_links = get_available_and_fetched_links()
        params = get_query_parameters(start=fetched_links, day=day)
        while keep_querying_for_imagery:
            results = 


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


def query_for_imagery(
    query_params: dict,
) -> List[ScihubResult]:
    query_date = date.strftime("%Y:%m:%d")
    params = {
        "q": (
            "(platformname:Sentinel-2) AND (processinglevel:Level-1C) AND "
            f"(ingestiondate:[{query_date}T00:00:00Z TO {query_date}T23:59:59Z])"
        ),
        "rows": 100,
        "format": "json",
        "orderby": "ingestiondate desc",
        "start": start_result,
    }
    resp = requests.get(
        url="https://scihub.copernicus.eu/dhus/search", params=params
    )
    resp.raise_for_status()

    results = resp.json()["feed"]
    total_results = int(results["opensearch:totalResults"])
