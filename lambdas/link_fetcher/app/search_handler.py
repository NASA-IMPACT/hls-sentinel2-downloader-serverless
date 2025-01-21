import os
from datetime import date, datetime, timedelta
from typing import (
    Any,
    Final,
    Mapping,
    Protocol,
    Sequence,
    Tuple,
    TypedDict,
)

import humanfriendly
import iso8601
import requests
from db.models.granule_count import GranuleCount
from db.models.status import Status
from db.session import get_session_maker

from app.common import (
    SearchResult,
    SessionMaker,
    add_search_results_to_db_and_sqs,
    filter_search_results,
    get_accepted_tile_ids,
    parse_tile_id_from_title,
)

MIN_REMAINING_MILLIS: Final = 60_000
SEARCH_URL: Final = os.environ.get(
    "SEARCH_URL",
    "https://catalogue.dataspace.copernicus.eu",
)


class Context(Protocol):
    def get_remaining_time_in_millis(self) -> int: ...


class HandlerResult(TypedDict):
    query_date_platform: Tuple[str, str]
    completed: bool


def handler(event: Mapping[str, Any], context: Context) -> HandlerResult:
    return _handler(event, context, get_session_maker())


def _handler(
    event: Mapping[str, Any],
    context: Context,
    session_maker: SessionMaker,
) -> HandlerResult:
    accepted_tile_ids = get_accepted_tile_ids()
    query_date, query_platform = event["query_date_platform"]
    day = datetime.strptime(query_date, "%Y-%m-%d").date()

    fetched_links = get_fetched_links(session_maker, day, query_platform)
    params = get_query_parameters(fetched_links, day, query_platform)
    search_results, total_results = get_page_for_query_and_total_results(params)
    print(
        f"Previously fetched links for {query_date}/{query_platform}: {fetched_links}/{total_results}"
    )
    update_total_results(session_maker, day, query_platform, total_results)
    bail_early = False

    while search_results:
        number_of_fetched_links = len(search_results)
        filtered_search_results = filter_search_results(
            search_results, accepted_tile_ids
        )
        add_search_results_to_db_and_sqs(session_maker, filtered_search_results)
        update_last_fetched_link_time(session_maker)
        update_fetched_links(
            session_maker, day, query_platform, number_of_fetched_links
        )

        params = {**params, "index": params["index"] + number_of_fetched_links}
        print(
            f"Fetched links for {query_date}/{query_platform}: {params['index'] - 1}/{total_results}"
        )

        if bail_early := context.get_remaining_time_in_millis() < MIN_REMAINING_MILLIS:
            print("Bailing early to avoid Lambda timeout")
            break

        search_results, _ = get_page_for_query_and_total_results(params)

    return {
        "query_date_platform": (query_date, query_platform),
        "completed": not bail_early,
    }


def get_fetched_links(session_maker: SessionMaker, day: date, platform: str) -> int:
    """
    For a given day, return the total
    `fetched_links`, where `fetched_links` is the total number of granules that have
    been processed (but not necessarily added to the database because of filtering)

    If no entry is found, one is created
    :param session_maker: sessionmaker representing the SQLAlchemy sessionmaker to use
        for database interactions
    :param day: date representing the day to return results for
    :param platform: Sentinel-2 platform to search for (S2A, S2B, etc)
    :returns: int representing `fetched_links`
    """
    with session_maker() as session:
        granule_count = (
            session.query(GranuleCount).filter_by(date=day, platform=platform).first()
        )

        if granule_count:
            return granule_count.fetched_links

        granule_count = GranuleCount(
            date=day,
            platform=platform,
            available_links=0,
            fetched_links=0,
            last_fetched_time=datetime.now(),
        )  # type: ignore
        session.add(granule_count)
        session.commit()

        return 0


def update_total_results(
    session_maker: SessionMaker, day: date, platform: str, total_results: int
):
    """
    For a given day and number of results, update the `available_links` value
    :param session_maker: sessionmaker representing the SQLAlchemy sessionmaker to use
        for database interactions
    :param day: date representing the day to update `available_links` for
    :param platform: Sensor platform (S2A, S2B, etc)
    :param total_results: int representing the total results available for the day,
        this value will be applied to `available_links`
    """
    with session_maker() as session:
        if (
            granule_count := session.query(GranuleCount)
            .filter_by(date=day, platform=platform)
            .first()
        ):
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


def update_fetched_links(
    session_maker: SessionMaker, day: date, platform: str, fetched_links: int
):
    """
    For a given day, update the `fetched_links` value in `granule_count` to the provided
    `fetched_links` value and update the `last_fetched_time` value to `datetime.now()`
    :param session_maker: sessionmaker representing the SQLAlchemy sessionmaker to use
        for database interactions
    :param day: date representing the day to update in `granule_count`
    :param platform: Sentinel-2 platform to search for (S2A, S2B, etc)
    :param fetched_links: int representing the total number of links fetched in this run
        it is not the total number of Granules created
    """
    with session_maker() as session:
        if (
            granule_count := session.query(GranuleCount)
            .filter_by(date=day, platform=platform)
            .first()
        ):
            granule_count.fetched_links += fetched_links
            granule_count.last_fetched_time = datetime.now()
            session.commit()


def get_query_parameters(start: int, day: date, platform: str) -> Mapping[str, Any]:
    """
    Returns the query parameters that are needed for getting new imagery from
    search (Copernicus Data Space Ecosystem)

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
        "platform": platform,
        "sortParam": "published",
        "sortOrder": "desc",
        "maxRecords": 2000,
        # `start` is 0-based, but `index` is 1-based, so we must add 1
        "index": start + 1,
        # Fix for issue #28, due to breaking change in the OpenSearch API
        # Search for "update of the exactCount parameter" at the following URL:
        # https://documentation.dataspace.copernicus.eu/APIs/ReleaseNotes.html#opensearch-api-error-handling-update-2023-10-24
        "exactCount": 1,
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
    tile_id = parse_tile_id_from_title(title)

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


def get_page_for_query_and_total_results(
    query_params: Mapping[str, Any],
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

    resp = requests.get(
        f"{SEARCH_URL}/resto/api/collections/Sentinel2/search.json",
        params=query_params,
    )
    print(f"Search URL: {resp.url}")
    resp.raise_for_status()
    results = resp.json()
    # If totalResults is either missing or present but set to None, default it to -1
    total_results = results["properties"].get("totalResults", -1) or -1

    search_results = tuple(
        create_search_result(entry) for entry in results.get("features", [])
    )

    return search_results, total_results
