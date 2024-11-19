import json
from typing import Callable, Sequence
from unittest.mock import patch

from assertpy import assert_that
from db.models.granule import Granule
from sqlalchemy.orm import Session

from app.common import (
    SearchResult,
    add_search_result_to_sqs,
    add_search_results_to_db_and_sqs,
    get_accepted_tile_ids,
)


def test_that_link_fetcher_handler_correctly_loads_allowed_tiles():
    tile_ids = get_accepted_tile_ids()
    assert_that(tile_ids).is_length(18952)
    assert_that(tile_ids).contains("01FBE")
    assert_that(tile_ids).contains("60WWV")


def test_that_link_fetcher_handler_correctly_adds_search_results_to_db(
    db_session: Session,
    search_result_maker: Callable[[int], Sequence[SearchResult]],
    mock_sqs_queue,
    db_session_context,
):
    search_results = search_result_maker(10)
    search_result_id_base = search_results[0].image_id[:-3]
    search_result_url_base = search_results[0].download_url[:-3]

    with patch("app.common.add_search_result_to_sqs") as mock_add_to_sqs:
        mock_add_to_sqs.return_value = None
        add_search_results_to_db_and_sqs(lambda: db_session, search_results)
        mock_add_to_sqs.assert_called()

    granules_in_db = db_session.query(Granule).all()
    assert_that(granules_in_db).is_length(10)

    for idx, granule in enumerate(granules_in_db):
        id_filled = str(idx).zfill(3)
        expected_id = f"{search_result_id_base}{id_filled}"
        expected_url = f"{search_result_url_base}{id_filled}"
        granule_id = granule.id
        granule_download_url = granule.download_url
        assert_that(expected_id).is_equal_to(granule_id)
        assert_that(expected_url).is_equal_to(granule_download_url)


def test_that_link_fetcher_handler_correctly_handles_duplicate_db_entry(
    db_session: Session,
    search_result_maker: Callable[[int], Sequence[SearchResult]],
    mock_sqs_queue,
):
    search_result = search_result_maker(1)[0]
    db_session.add(
        Granule(
            id=search_result.image_id,
            filename=search_result.filename,
            tileid=search_result.tileid,
            size=search_result.size,
            beginposition=search_result.beginposition,
            endposition=search_result.endposition,
            ingestiondate=search_result.ingestiondate,
            download_url=search_result.download_url,
        )  # type: ignore
    )

    # Because of how the db sessions are handled in these unit tests, the rollback
    # call actually undoes the insert that the test setup does, so we're just asserting
    # that rollback is called, not that there is still one entry.
    # That's the best we can do without a full e2e test.
    with patch.object(db_session, "rollback") as rollback:
        add_search_results_to_db_and_sqs(lambda: db_session, [search_result])
        rollback.assert_called_once()


def test_that_link_fetcher_handler_correctly_adds_search_result_to_queue(
    mock_sqs_queue,
    search_result_maker: Callable[[int], Sequence[SearchResult]],
    sqs_client,
):
    search_result = search_result_maker(1)[0]
    search_result_id = search_result.image_id
    search_result_url = search_result.download_url
    search_result_filename = search_result.filename

    add_search_result_to_sqs(search_result, sqs_client, mock_sqs_queue.url)

    mock_sqs_queue.load()

    number_of_messages_in_queue = mock_sqs_queue.attributes[
        "ApproximateNumberOfMessages"
    ]
    assert_that(int(number_of_messages_in_queue)).is_equal_to(1)

    message = mock_sqs_queue.receive_messages(MaxNumberOfMessages=1)[0]
    message_body = json.loads(message.body)
    assert_that(search_result_id).is_equal_to(message_body["id"])
    assert_that(search_result_url).is_equal_to(message_body["download_url"])
    assert_that(search_result_filename).is_equal_to(message_body["filename"])
