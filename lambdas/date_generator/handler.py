import os
from collections.abc import Sequence
from datetime import datetime, timedelta
from itertools import product
from typing import TypedDict


class Event(TypedDict, total=False):
    """Input event payload

    These inputs are not required but can be provided to override
    behavior for unit testing or backfills. Defaults are set inside
    of the handler function.
    """

    platforms: Sequence[str]
    now: str
    lookback_days: int


DATE_FORMAT_YMD = "%Y-%m-%d"
DEFAULT_LOOKBACK_DAYS = 5


def handler(
    event: Event,
    _context,
):
    """
    Return a `dict` with the single key `'query_dates_platforms'` mapped to a list of
    2-tuples of the form `(date, platform)` produced from the cross-product of dates
    given by `get_dates` (for `event['lookback_days']` number of days _prior_ to the
    date given by `event['now']`) and `event['platforms']`.

    NOTE: Our StepFunction will never pass these kwargs in the payload by default.
    They are for backfill and unit testing purposes only, but since they are all
    optional, our daily scheduled StepFunction can safely call this function with none
    of them.

    Raises
    ------
    KeyError: if `platforms` is not specified in the handler payload and the environment
        variable `PLATFORMS` is not defined
    ValueError: if the `platforms` input is an empty sequence, or it is not specified
        and the `PLATFORMS` environment variable is set to a value that is either
        empty, only whitespace, or a combination of commas and whitespace

    Examples
    --------
    The number of date-platform pairs should be the number of lookback days times the
    number of platforms, regardless of the current date:

    >>> platforms = ("S2A", "S2B", "S2C")
    >>> combos = handler({"platforms": platforms}, None)["query_dates_platforms"]
    >>> len(combos) == DEFAULT_LOOKBACK_DAYS * len(platforms)
    True

    For a known date and number of lookback days, we can enumerate the exact combos:

    >>> handler(  # doctest: +NORMALIZE_WHITESPACE
    ...     {"platforms": platforms, "now": "2024-03-02", "lookback_days": 3},
    ...     None,
    ... )
    {'query_dates_platforms':
     [('2024-03-01', 'S2A'), ('2024-03-01', 'S2B'), ('2024-03-01', 'S2C'),
      ('2024-02-29', 'S2A'), ('2024-02-29', 'S2B'), ('2024-02-29', 'S2C'),
      ('2024-02-28', 'S2A'), ('2024-02-28', 'S2B'), ('2024-02-28', 'S2C')]}
    """
    # We want to fail if neither platforms is supplied as a kwarg (during testing) nor
    # PLATFORMS is defined as a (non-empty) environment variable.
    platforms = event.get("platforms") or parse_platforms(os.environ["PLATFORMS"])
    # By default "now" should be today to support cron usage, but allow overrides
    # for backfill jobs
    now = datetime.strptime(
        event.get("now", datetime.now().strftime(DATE_FORMAT_YMD)), DATE_FORMAT_YMD
    )
    lookback_days = event.get("lookback_days", DEFAULT_LOOKBACK_DAYS)

    return {
        "query_dates_platforms": list(product(get_dates(now, lookback_days), platforms))
    }


def parse_platforms(platforms: str) -> Sequence[str]:
    """
    Parse a string into a sequence of strings, split around whitespace, commas, or
    a combination thereof.

    Raises
    ------
    ValueError: if `platforms` is an empty string, only whitespace, or a combination of
        only commas and whitespace

    Examples
    --------
    Some valid inputs:

    >>> parse_platforms("S2A")
    ('S2A',)
    >>> parse_platforms("S2A,S2B")
    ('S2A', 'S2B')
    >>> parse_platforms("S2A, S2B , S2C")
    ('S2A', 'S2B', 'S2C')
    >>> parse_platforms("S2A S2B  S2C")
    ('S2A', 'S2B', 'S2C')
    >>> parse_platforms("S2A S2B , S2C")
    ('S2A', 'S2B', 'S2C')

    Some invalid inputs:

    >>> parse_platforms("")
    Traceback (most recent call last):
        ...
    ValueError: empty platforms list
    >>> parse_platforms("  ")
    Traceback (most recent call last):
        ...
    ValueError: empty platforms list
    >>> parse_platforms(" , ")
    Traceback (most recent call last):
        ...
    ValueError: empty platforms list
    """

    import re

    if not (result := tuple(filter(None, re.split(r"\s*,\s*|\s+", platforms)))):
        raise ValueError("empty platforms list")

    return result


def get_dates(now: datetime, lookback_days: int) -> Sequence[str]:
    """
    Return one date string per day for `lookback_days` number of days, in reverse
    chronological order, starting from the day before `now` and formatted as
    `%Y-%m-%d`.

    Examples
    --------
    >>> len(get_dates(datetime.now(), 10)) == 10
    True
    >>> get_dates(datetime(2025, 1, 3), 3)
    ['2025-01-02', '2025-01-01', '2024-12-31']

    :returns: string dates (`%Y-%m-%d`) looking back the number of days given by
        `lookback_days` in reverse chronological order starting from the day before
        `now()`
    """
    yesterdays_date = now.date() - timedelta(days=1)
    return [
        (yesterdays_date - timedelta(days=day)).strftime(DATE_FORMAT_YMD)
        for day in range(lookback_days)
    ]
