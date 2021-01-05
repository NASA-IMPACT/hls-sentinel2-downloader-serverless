#!/usr/bin/env python3
import os

from aws_cdk import core

from app.downloader_stack import DownloaderStack

app = core.App()

identifier = os.environ["IDENTIFIER"]

DownloaderStack(
    app, f"hls-s2-downloader-serverless-{identifier}", identifier=identifier
)

for k, v in {
    "Project": "hls-s2-downloader-serverless",
    "Stack": os.environ["STAGE"],
    "Client": "nasa-impact",
    "Owner": os.environ["OWNER"],
}.items():
    core.Tags.of(app).add(k, v, apply_to_launched_instances=True)

app.synth()