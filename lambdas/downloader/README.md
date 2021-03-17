# Downloader 💾

## High level overview

![Downloader in S2 Downloader diagram](../../images/hls-s2-downloader-downloader.png)

The Downloaders purpose is download Sentinel 2 Images from Sci/IntHub. It is invoked via SQS messages being available within the `TO_DOWNLOAD` SQS queue; this handler will be limited to a concurrency limit of 15, due to the nature of the dedicated connection we have to IntHub. Images are downloaded and uploaded to S3 in one step, they are stored under a key in the format `<YYYY-MM-DD>/<image_filename>` where the date is the `beginposition` of the image.

S3 performs a MD5 checksum comparison on upload, this way we ensure that we only store images that match the MD5 that Sci/IntHub provided us for the image.

Interactions with the `granule` table include marking the download as having started, updating the checksum of the image, and marking that the download is complete.

---

## Handler breakdown

Provided below is some pseudo-code to explain the process happening each time the lambda is invoked:

```python
message_contents = get_message_contents()

try:
    granule = get_granule_for_message_and_set_to_downloading()
except NotFound:
    return # End gracefully - If it wasn't in granules, we dont want to download it anyway
except AlreadyDownloaded:
    return # End gracefully - We received a duplicate from SQS, this is OK

try:
    checksum = get_checksum_from_scihub()
    download_file()
except Exception as ex:
    increase_retry_count()
    raise ex # Any caught exception here is a 'true' error, we want to fail and retry the image

update_status()
```

### Notes:

Due to the nature of how Lambda is invoked by SQS, a non-failed invocation of a Lambda will result in the SQS message being deleted. Because of this, if we need to gracefully handle an error, we tidy up (namely database rollbacks), then raise the error to the handler, this then results in the Lambda failing and the SQS message being re-added to the Queue.

We use the flag `USE_INTHUB2` with possible values of `YES` and `NO` to determine whether we:
* A - Replace `scihub` in the fetched links download urls with `inthub2`
* B - Retrieve `inthub2` credentials when downloading files

---

## Development

This Lambda makes use of `pipenv` for managing depedencies and for building the function when deploying it.

To get setup for developing this project, run:

```bash
$ pipenv install --dev
```

_**Note** if you don't have `PIPENV_NO_INHERIT=TRUE` in your env vars, you will need to prepend the above command with it, to make sure you create a `pipenv` `venv` for just this directory._

This Lambda makes use of the `db` module that will be available via a Lambda Layer once deployed. For local development purposes, it is installed as a editable relative `[dev-packages]` dependency

---

### .env

This Lambda requires a `.env` file in its directory containing the following env vars:

```
PG_PASSWORD="<any-value>"
PG_USER="<any-value>"
PG_DB="<any-value>"
```

This is used whilst running the tests to provide both the Postgres container and the test code the credentials needed to access the database created.

---

### Makefile

A `Makefile` is provided to abstract commonly used commands away:

**`make install`**

> This will run `pipenv install --dev` to install development dependencies

**`make lint`**

> This will perform a dry run of `flake8`, `isort`, and `black` and let you know what issues were found

**`make format`**

> This will peform a run of `isort` and `black`, this **will** modify files if issues were found

**`make test`**

> This will run the unit tests of the project with `pytest` using the contents of your `.env` file

---

