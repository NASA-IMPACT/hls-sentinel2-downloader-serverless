import base64
from pathlib import Path
from typing import Any, Mapping


def handler(event: Mapping[str, Any], _) -> Mapping[str, Any]:
    print(event)
    product = event.get("pathParameters", {}).get("product")
    filterParam = (event.get("queryStringParameters") or {}).get("$filter")
    fixtures_dir = Path(__file__).parent / "scihub_responses"

    if filterParam == "Id eq 'integration-test-id'":
        body = (fixtures_dir / "scihub_response_mock_image_checksum.json").read_text()
        return {"statusCode": 200, "body": body}

    if product == "Products(integration-test-id)/$value":
        fixture = (fixtures_dir / "scihub_response_mock_image.SAFE").read_bytes()
        body = base64.b64encode(fixture).decode("utf-8")

        return {
            "isBase64Encoded": True,
            "statusCode": 200,
            "body": body,
            "headers": {
                "Content-Type": "application/octet-stream",
                "Content-Disposition": 'attachment; filename="blah.SAFE"',
            },
        }

    return {"statusCode": 404}
