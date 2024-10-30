#!/usr/bin/env python3
import os

from aws_cdk import App, Tags
from dotenv import load_dotenv
from downloader_stack import DownloaderStack
from integration_stack import IntegrationStack

load_dotenv(override=True)
app = App()

identifier = os.environ["IDENTIFIER"].replace("/", "")
permissions_boundary_arn = os.getenv("PERMISSIONS_BOUNDARY_ARN")

integration_stack = IntegrationStack(
    app,
    f"hls-s2-downloader-serverless-integration-{identifier}",
    identifier=identifier,
    permissions_boundary_arn=permissions_boundary_arn,
)

downloader_stack = DownloaderStack(
    app,
    f"hls-s2-downloader-serverless-{identifier}",
    identifier=identifier,
    upload_bucket=integration_stack.upload_bucket.bucket_name,
    permissions_boundary_arn=permissions_boundary_arn,
    search_url=integration_stack.scihub_url,
    zipper_url=integration_stack.scihub_url,
    checksum_url=integration_stack.scihub_url,
)

integration_stack.upload_bucket.grant_put(downloader_stack.downloader)

for k, v in {
    "Project": "hls-s2-downloader-serverless",
    "Stack": identifier,
    "Client": "nasa-impact",
    "Owner": os.environ["OWNER"],
    "Commit": os.environ.get("COMMIT", "N/A"),
}.items():
    Tags.of(app).add(k, v, apply_to_launched_instances=True)

app.synth()
