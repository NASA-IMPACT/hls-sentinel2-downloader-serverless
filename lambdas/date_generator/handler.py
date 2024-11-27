from datetime import datetime, timedelta
from itertools import product
from typing import List


def handler(event, context):
    platforms = ["S2A", "S2B"]
    return {
        "query_dates_platforms": list(product(get_dates(), platforms)),
    }


def get_dates() -> List[str]:
    """
    Returns 5 date strings from `datetime.now() - 1 day` with the latest day first
    Strings are formatted as %Y-%m-%d
    :returns: List[str] representing 5 days from yesterday
    """
    yesterdays_date = datetime.now().date() - timedelta(days=1)
    return [
        (yesterdays_date - timedelta(days=day)).strftime("%Y-%m-%d")
        for day in range(0, 5)
    ]
