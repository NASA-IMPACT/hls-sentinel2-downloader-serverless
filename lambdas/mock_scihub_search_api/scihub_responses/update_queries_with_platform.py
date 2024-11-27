#!/usr/bin/env python
import json
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

import requests


def main():
    HERE = Path(__file__).parent

    response_paths = [
        path
        for path in HERE.glob("scihub_response_*.json")
        if "platform" not in path.name
    ]

    for response_path in response_paths:
        # Read query
        data = json.load(response_path.open())
        url = [link for link in data["properties"]["links"] if link["rel"] == "self"][
            0
        ]["href"]

        # Update with `platform=<platform>`
        for platform in ("S2A", "S2B"):
            parsed = urlparse(url)
            query_params = parse_qs(parsed.query)
            query_params["platform"] = platform
            query_params["exactCount"] = 1

            url_with_platform = parsed._replace(
                query=urlencode(query_params, doseq=True)
            ).geturl()

            # Get new response
            req = requests.get(url_with_platform)

            # Write
            updated_path = HERE / response_path.name.replace(
                "scihub_response_", f"scihub_response_platform_{platform}_"
            )
            updated_path.write_text(json.dumps(req.json()))


if __name__ == "__main__":
    main()
