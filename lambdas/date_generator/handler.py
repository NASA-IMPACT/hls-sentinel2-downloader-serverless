import os
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta
from itertools import product
from typing import TypedDict, Unpack


class HandlerKwargs(TypedDict, total=False):
    platforms: Sequence[str]
    now: Callable[[], datetime]
    lookback_days: int


DEFAULT_LOOKBACK_DAYS = 5


def handler(
    _event,
    _context,
    **kwargs: Unpack[HandlerKwargs],
):
    """
    Return a `dict` with the single key `'query_dates_platforms'` mapped to a list of
    2-tuples of the form `(date, platform)` produced from the cross-product of dates
    given by `get_dates` (for `kwargs['lookback_days']` number of days _prior_ to the
    date given by `kwargs['now']()`) and `kwargs['platforms']`.

    NOTE: AWS will never pass kwargs.  They are for unit testing purposes only, but
    since they are all optional, AWS can safely call this function with none of them.

    Raises
    ------
    KeyError: if the `platforms` kwarg is not specified and the environment variable
        `PLATFORMS` is not defined
    ValueError: if the `platforms` kwarg is an empty sequence, or it is not specified
        and the `PLATFORMS` environment variable is set to a value that is either
        empty, only whitespace, or a combination of commas and whitespace

    Examples
    --------
    The number of date-platform pairs should be the number of lookback days times the
    number of platforms, regardless of the current date:

    >>> platforms = ("S2A", "S2B", "S2C")
    >>> combos = handler(None, None, platforms=platforms)["query_dates_platforms"]
    >>> len(combos) == DEFAULT_LOOKBACK_DAYS * len(platforms)
    True

    For a known date and number of lookback days, we can enumerate the exact combos:

    >>> handler(  # doctest: +NORMALIZE_WHITESPACE
    ...     None,
    ...     None,
    ...     platforms=platforms,
    ...     now=lambda: datetime(2024, 3, 2),
    ...     lookback_days=3
    ... )
    {'query_dates_platforms':
     [('2024-03-01', 'S2A'), ('2024-03-01', 'S2B'), ('2024-03-01', 'S2C'),
      ('2024-02-29', 'S2A'), ('2024-02-29', 'S2B'), ('2024-02-29', 'S2C'),
      ('2024-02-28', 'S2A'), ('2024-02-28', 'S2B'), ('2024-02-28', 'S2C')]}
    """

    default_kwargs: HandlerKwargs = {
        "now": datetime.now,
        "lookback_days": DEFAULT_LOOKBACK_DAYS,
    }
    kwargs = default_kwargs | kwargs

    # We want to fail if neither platforms is supplied as a kwarg (during testing) nor
    # PLATFORMS is defined as a (non-empty) environment variable.
    platforms = kwargs.get("platforms") or parse_platforms(os.environ["PLATFORMS"])
    now = kwargs["now"]
    lookback_days = kwargs["lookback_days"]

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


def get_dates(now: Callable[[], datetime], lookback_days: int) -> Sequence[str]:
    """
    Return one date string per day for `lookback_days` number of days, in reverse
    chronological order, starting from the day before `now()` and formatted as
    `%Y-%m-%d`.

    Examples
    --------
    >>> len(get_dates(datetime.now, 10)) == 10
    True
    >>> get_dates(lambda: datetime(2025, 1, 3), 3)
    ['2025-01-02', '2025-01-01', '2024-12-31']

    :returns: string dates (`%Y-%m-%d`) looking back the number of days given by
        `lookback_days` in reverse chronological order starting from the day before
        `now()`
    """
    yesterdays_date = now().date() - timedelta(days=1)
    return [
        (yesterdays_date - timedelta(days=day)).strftime("%Y-%m-%d")
        for day in range(lookback_days)
    ]
