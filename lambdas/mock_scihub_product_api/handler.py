import base64
import re
from pathlib import Path
from typing import Any, Mapping


def handler(event: Mapping[str, Any], _) -> Mapping[str, Any]:
    print(event)
    # We're using "or {}" rather than supplying a default value to get() because
    # the keys are still present even when the values are None.  We're also
    # keeping get() in place rather than using direct indexing, just to be on
    # the safe side.
    product = (event.get("pathParameters") or {}).get("product", "")
    filter_param = (event.get("queryStringParameters") or {}).get("$filter", "")

    fixtures_dir = Path(__file__).parent / "scihub_responses"

    if product == "Products" and re.fullmatch("Id eq '.+'", filter_param):
        body = (fixtures_dir / "scihub_response_mock_image_checksum.json").read_text()
        return {"statusCode": 200, "body": body}

    if re.fullmatch("Products(.+)/[$]value", product):
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
