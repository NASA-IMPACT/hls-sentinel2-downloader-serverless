#!/usr/bin/env python3
import os

from aws_cdk import core
from downloader_stack import DownloaderStack
from integration_stack import IntegrationStack

app = core.App()

identifier = os.environ["IDENTIFIER"].replace("/", "")

integration_stack = IntegrationStack(
    app, f"hls-s2-downloader-serverless-integration-{identifier}", identifier=identifier
)

DownloaderStack(
    app,
    f"hls-s2-downloader-serverless-{identifier}",
    identifier=identifier,
    scihub_url=integration_stack.scihub_url,
)

for k, v in {
    "Project": "hls-s2-downloader-serverless",
    "Stack": identifier,
    "Client": "nasa-impact",
    "Owner": os.environ["OWNER"],
    "Commit": os.environ.get("COMMIT", "N/A"),
}.items():
    core.Tags.of(app).add(k, v, apply_to_launched_instances=True)

app.synth()
