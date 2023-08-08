from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class ScihubResult:
    image_id: str
    filename: str
    tileid: str
    size: int
    beginposition: datetime
    endposition: datetime
    ingestiondate: datetime
    download_url: str
