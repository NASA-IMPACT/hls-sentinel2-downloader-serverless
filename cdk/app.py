#!/usr/bin/env python3
import os

from aws_cdk import App, Tags
from dotenv import load_dotenv
from downloader_stack import DownloaderStack

load_dotenv(override=True)
app = App()

identifier = os.environ["IDENTIFIER"].replace("/", "")
upload_bucket = os.environ["UPLOAD_BUCKET"]
enable_downloading = os.environ["ENABLE_DOWNLOADING"] == "TRUE"
schedule_link_fetching = os.environ["SCHEDULE_LINK_FETCHING"] == "TRUE"
use_inthub2 = os.environ["USE_INTHUB2"] == "TRUE"
removal_policy_destroy = os.environ["REMOVAL_POLICY_DESTROY"] == "TRUE"
print(identifier)
DownloaderStack(
    app,
    f"hls-s2-downloader-serverless-{identifier}",
    identifier=identifier,
    upload_bucket=upload_bucket,
    enable_downloading=enable_downloading,
    use_inthub2=use_inthub2,
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
