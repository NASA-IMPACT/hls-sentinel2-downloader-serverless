from datetime import datetime, timedelta
from pathlib import Path


def handler(event, _):
    print(event)
    yesterday = datetime.now().date() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    params = event["queryStringParameters"]

    if yesterday_str not in params["publishedAfter"]:
        response_fixture = "scihub_response_no_results.json"
    elif (index := int(params["index"])) == 1:
        response_fixture = "scihub_response_index_1_yesterday.json"
    elif index == 101:
        response_fixture = "scihub_response_index_101_yesterday.json"
    else:
        response_fixture = "scihub_response_no_results.json"

    responses_dir = Path(__file__).with_name("scihub_responses")
    body = (responses_dir / response_fixture).read_text()

    return {"statusCode": "200", "body": body}
