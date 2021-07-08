import json
from datetime import datetime, timezone

from assertpy import assert_that
from db.models.granule import Granule
from db.models.status import Status
from db.session import get_session, get_session_maker


def test_that_downloader_correctly_downloads_file_and_updates_database(
    db_setup, mock_scihub_api_url, lambda_client, downloader_arn, upload_bucket
):
    image_download_url_part = "/dhus/odata/v1/Products('integration-test-id')/$value"
    image_download_url = f"{mock_scihub_api_url}{image_download_url_part}"

    now = datetime.now(timezone.utc)
    session_maker = get_session_maker()
    with get_session(session_maker) as db:
        db.add(
            Granule(
                id="integration-test-id",
                filename="integration-test-filename.SAFE",
                tileid="TS101",
                size=100,
                beginposition=now,
                endposition=now,
                ingestiondate=now,
                download_url=image_download_url,
            )
        )
        db.commit()

    invocation_body = json.dumps(
        {
            "Records": [
                {
                    "body": json.dumps(
                        {
                            "id": "integration-test-id",
                            "filename": "integration-test-filename.SAFE",
                            "download_url": image_download_url,
                        }
                    )
                }
            ]
        }
    )

    before_invocation = datetime.now(timezone.utc)
    _ = lambda_client.invoke(
        FunctionName=downloader_arn,
        InvocationType="RequestResponse",
        Payload=invocation_body,
    )
    after_invocation = datetime.now(timezone.utc)

    with get_session(session_maker) as db:
        granule = db.query(Granule).filter(Granule.id == "integration-test-id").first()
        assert_that(granule.downloaded).is_true()
        assert_that(granule.checksum).is_equal_to("ACD23199F98D2333A87013C047E43F62")

        status = (
            db.query(Status)
            .filter(Status.key_name == "last_file_downloaded_time")
            .first()
        )
        assert_that(
            datetime.strptime(status.value, "%Y-%m-%d %H:%M:%S.%f").replace(
                tzinfo=timezone.utc
            )
        ).is_between(before_invocation, after_invocation)

    today_str = now.strftime("%Y-%m-%d")
    bucket_objects = list(upload_bucket.objects.all())
    assert_that(bucket_objects).is_length(1)
    assert_that(bucket_objects[0].key).is_equal_to(
        f"{today_str}/integration-test-filename.SAFE"
    )
