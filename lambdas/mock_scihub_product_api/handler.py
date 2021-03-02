import base64
import json
import os


def handler(event, context):
    if (
        "pathParameters" in event
        and "product" in event["pathParameters"]
    ):
        if event["pathParameters"]["product"] == "Products('integration-test-id')":
            with open(
                os.path.join(
                    ".", "scihub_responses", "scihub_response_mock_image_checksum.json"
                ),
                "rb",
            ) as file_in:
                body = json.dumps(json.load(file_in))
            return {"statusCode": 200, "body": body}
        elif (
            event["pathParameters"]["product"]
            == "Products('integration-test-id')/$value"
        ):
            with open(
                os.path.join(
                    ".", "scihub_responses", "scihub_response_mock_image.SAFE"
                ),
                "rb",
            ) as file_in:
                body = base64.b64encode(file_in.read()).decode("utf-8")
            return {
                "isBase64Encoded": True,
                "statusCode": 200,
                "body": body,
                "headers": {
                    "Content-Type": "application/octet-stream",
                    "Content-Disposition": 'attachment; filename="blah.SAFE"',
                },
            }
        else:
            return {"statusCode": 404}
    else:
        return {"statusCode": 404}
