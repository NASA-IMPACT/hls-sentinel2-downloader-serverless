import json
from datetime import datetime, timezone

from db.models.granule import Granule
from db.models.status import Status
from mypy_boto3_lambda import LambdaClient
from mypy_boto3_s3.service_resource import Bucket
from sqlalchemy.orm import Session


def test_that_downloader_correctly_downloads_file_and_updates_database(
    db_session: Session,
    mock_scihub_api_url: str,
    lambda_client: LambdaClient,
    downloader_arn: str,
    upload_bucket: Bucket,
):
    image_download_url_path = "/dhus/odata/v1/Products('integration-test-id')/$value"
    image_download_url = f"{mock_scihub_api_url}{image_download_url_path}"

    before_invocation = datetime.now(timezone.utc)

    db_session.add(
        Granule(
            id="integration-test-id",
            filename="integration-test-filename.SAFE",
            tileid="TS101",
            size=100,
            beginposition=before_invocation,
            endposition=before_invocation,
            ingestiondate=before_invocation,
            download_url=image_download_url,
        )
    )
    db_session.commit()

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

    lambda_client.invoke(
        FunctionName=downloader_arn,
        InvocationType="RequestResponse",
        Payload=invocation_body,
    )

    granule = (
        db_session.query(Granule).filter(Granule.id == "integration-test-id").first()
    )
    print(f"{granule=}")
    print(db_session.query(Status).all())
    last_file_downloaded_time = datetime.strptime(
        db_session.query(Status)
        .filter(Status.key_name == "last_file_downloaded_time")
        .first()
        .value,
        "%Y-%m-%d %H:%M:%S.%f",
    ).replace(tzinfo=timezone.utc)

    after_invocation = datetime.now(timezone.utc)

    assert granule.downloaded
    assert granule.checksum == "ACD23199F98D2333A87013C047E43F62"
    assert before_invocation <= last_file_downloaded_time <= after_invocation

    bucket_objects = list(upload_bucket.objects.all())

    assert len(bucket_objects) == 1
    assert bucket_objects[0].key == "integration-test-filename.zip"
