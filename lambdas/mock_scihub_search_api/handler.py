import json
import os
from datetime import datetime, timedelta

RESPONSES_DIR = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), "scihub_responses"
)


def handler(event, context):
    yesterday = datetime.now().date() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    if (
        "queryStringParameters" in event
        and "q" in event["queryStringParameters"]
        and "start" in event["queryStringParameters"]
    ):
        if yesterday_str in event["queryStringParameters"]["q"]:
            start = int(event["queryStringParameters"]["start"])
            if start == 0:
                with open(
                    os.path.join(
                        RESPONSES_DIR, "scihub_response_start_0_yesterday.json"
                    )
                ) as json_in:
                    body = json.load(json_in)
            elif start == 100:
                with open(
                    os.path.join(
                        RESPONSES_DIR, "scihub_response_start_100_yesterday.json"
                    )
                ) as json_in:
                    body = json.load(json_in)
            else:
                with open(
                    os.path.join(
                        RESPONSES_DIR, "scihub_response_no_results_yesterday.json"
                    )
                ) as json_in:
                    body = json.load(json_in)
        else:
            with open(
                os.path.join(RESPONSES_DIR, "scihub_response_no_results.json")
            ) as json_in:
                body = json.load(json_in)
    return {"statusCode": "200", "body": json.dumps(body)}
