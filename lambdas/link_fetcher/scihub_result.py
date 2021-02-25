from datetime import datetime
from typing import TypedDict


class ScihubResult(TypedDict):
    image_id: str
    filename: str
    tileid: str
    size: int
    beginposition: datetime
    endposition: datetime
    ingestiondate: datetime
    download_url: str
