from datetime import datetime, timedelta
from typing import List


def handler(event, context):
    return {"query_dates": get_dates()}


def get_dates() -> List[str]:
    """
    Returns 21 date strings from `datetime.now() - 1 day` with the latest day first
    Strings are formatted as %Y-%m-%d
    :returns: List[str] representing 21 days from yesterday
    """
    yesterdays_date = datetime.now().date() - timedelta(days=1)
    return [
        (yesterdays_date - timedelta(days=day)).strftime("%Y-%m-%d")
        for day in range(0, 21)
    ]
