# Date Generator 📆

## High level overview

![Date Generator in S2 Downloader diagram](../../images/hls-s2-downloader-date-generator.png)

The Date Generator's purpose is to generate a list of date strings in the form `YYYY-MM-DD` for 5 days from `today - 1` into the past along with the satellite platforms you want to download (S2A, S2B, S2C, etc).
This component is the "brains of the operation" and instructs other components about what data should be downloaded.
It is invoked within the `Link Fetching` Step Function on daily schedule for standard forward processing operations.
The output of this function looks like,

```json
{
    "query_dates_platform": [
        ["2025-01-28", "S2B"],
        ["2025-01-28", "S2C"],
        ["2025-01-27", "S2B"],
        ["2025-01-27", "S2C"],
        ["2025-01-26", "S2B"],
        ["2025-01-26", "S2C"],
        ["2025-01-25", "S2B"],
        ["2025-01-25", "S2C"],
        ["2025-01-24", "S2B"],
        ["2025-01-24", "S2C"],
    ]
}
```

It is also possible to invoke this function with specific parameters for backfilling missing data. For example you can invoke it for a specific time period and set of Sentinel-2 platforms by passing a payload into the StepFunction invocation,

```json
{
    "now": "2025-01-22",
    "lookback_days": 2,
    "platforms": ["S2B", "S2C"]
}
```

---

## Handler breakdown

Provided below is some pseudo-code to explain the process happening each time the lambda is invoked:

```python
yesterdays_date = get_yesterdays_date()
return generate_list_of_5_dates_into_the_past_from(yesterdays_date)
```

---

## Development

This Lambda makes use of `pipenv` for managing dependencies and for building the function when deploying it.

To get setup for developing this project, run:

```bash
$ pipenv install --dev
```

_**Note** if you don't have `PIPENV_NO_INHERIT=TRUE` in your env vars, you will need to prepend the above command with it, to make sure you create a `pipenv` `venv` for just this directory._

---

### Makefile

A `Makefile` is provided to abstract commonly used commands away:

**`make install`**

> This will run `pipenv install --dev` to install development dependencies

**`make lint`**

> This will perform a dry run of `flake8`, `isort`, and `black` and let you know what issues were found

**`make format`**

> This will perform a run of `isort` and `black`, this **will** modify files if issues were found

**`make test`**

> This will run the unit tests of the project with `pytest` using the contents of your `.env` file

---

