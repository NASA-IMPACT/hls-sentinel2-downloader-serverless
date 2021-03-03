#!/usr/bin/env python3
import os

from aws_cdk import core
from downloader_stack import DownloaderStack

app = core.App()

identifier = os.environ["IDENTIFIER"].replace("/", "")
upload_bucket = os.environ["UPLOAD_BUCKET"]

DownloaderStack(
    app,
    f"hls-s2-downloader-serverless-{identifier}",
    identifier=identifier,
    upload_bucket=upload_bucket,
)

for k, v in {
    "Project": "hls-s2-downloader-serverless",
    "Stack": identifier,
    "Client": "nasa-impact",
    "Owner": os.environ["OWNER"],
}.items():
    core.Tags.of(app).add(k, v, apply_to_launched_instances=True)

app.synth()
