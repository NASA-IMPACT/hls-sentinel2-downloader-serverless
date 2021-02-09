#!/usr/bin/env python3
import os

from aws_cdk import core

from downloader_stack import DownloaderStack

app = core.App()

identifier = os.environ["IDENTIFIER"]
stage = os.environ["STAGE"]

DownloaderStack(
    app,
    f"hls-s2-downloader-serverless-integration-{identifier}",
    identifier=identifier,
    stage=stage,
)

for k, v in {
    "Project": "hls-s2-downloader-serverless",
    "Stack": os.environ["STAGE"],
    "Client": "nasa-impact",
    "Owner": os.environ["OWNER"],
}.items():
    core.Tags.of(app).add(k, v, apply_to_launched_instances=True)

app.synth()
