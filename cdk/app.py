#!/usr/bin/env python3
import os

from aws_cdk import App, Tags
from dotenv import load_dotenv
from downloader_stack import DownloaderStack

load_dotenv(override=True)
app = App()

identifier = os.environ["IDENTIFIER"].replace("/", "")
permissions_boundary_arn = os.getenv("PERMISSIONS_BOUNDARY_ARN")
upload_bucket = os.environ["UPLOAD_BUCKET"]
enable_downloading = os.environ["ENABLE_DOWNLOADING"] == "TRUE"
schedule_link_fetching = os.environ["SCHEDULE_LINK_FETCHING"] == "TRUE"
removal_policy_destroy = os.environ["REMOVAL_POLICY_DESTROY"] == "TRUE"
platforms = os.environ["PLATFORMS"]
print(identifier)


DownloaderStack(
    app,
    f"hls-s2-downloader-serverless-{identifier}",
    identifier=identifier,
    upload_bucket=upload_bucket,
    platforms=platforms,
    permissions_boundary_arn=permissions_boundary_arn,
    enable_downloading=enable_downloading,
    schedule_link_fetching=schedule_link_fetching,
    removal_policy_destroy=removal_policy_destroy,
)

for k, v in {
    "Project": "hls-s2-downloader-serverless",
    "Stack": identifier,
    "Client": "nasa-impact",
    "Owner": os.environ["OWNER"],
}.items():
    Tags.of(app).add(k, v, apply_to_launched_instances=True)

app.synth()
