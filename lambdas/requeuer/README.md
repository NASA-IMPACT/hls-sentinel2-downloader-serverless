# Requeuer 🔗

The Requeuer is a standalone AWS Lambda function that will requeue granules with
a specified ingestion date, if they were not downloaded.  This allows us to
backfill missing granules where messages went to the DLQ, but expired before the
DLQ was redriven.

## Requeueing Missing Granules

Although this Lambda function is deployed as part of this repository's CDK
stack, it is not connected to anything, and is thus "standalone," requiring
manual execution.  Therefore, once deployed, you may trigger execution manually
either via the AWS Console or the AWS CLI.

The input is simply a single date to use for selecting granules in the database
with a matching value for `ingestiondate` (and with `downloaded` set to `False`),
along with a "dry run" indicator, and must be in the following form:

```json
{
  "dry_run": true | false,
  "date": "YYYY-MM-DD"
}
```

When `"dry_run"` is set to `true`, the missing (undownloaded) granules for the
specified date will be found, but they will _not_ be requeued for download.

Note that a value for `"dry_run"` is _required_ as a precaution to reduce the
likelihood of accidental requeuing, as might be the case if it were to default
to `false`, when not supplied.  Conversely, this may also reduce the chance of
confusion as to why granules are _not_ requeued, if it were to instead default
to `true`.

To make it easy to invoke the Lambda function from the command line, you may use
the provided `invoke` shell script.  For example, to perform a "dry run" for
granules ingested on June 10, 2023, you may run the script as follows:

```plain
./invoke --dry-run 2023-06-10 response.json
```

The output will be written to the indicated file (`response.json` in this case),
and will include the given inputs, as well as a list of all granules ingested
on the specified date that have not been downloaded.

**NOTE:** It is likely more convenient to run `invoke` from the root of the
repository, where you likely have a `.env` file with the `IDENTIFIER`
environment variable defined.  The `invoke` script will automatically source a
`.env` found in the current directory.  Therefore, from the root of the
repository, it might be more convenient to run the Lambda function as follows:

```plain
lambdas/requeuer/invoke --dry-run 2023-06-10 response.json
```

## Development

This Lambda makes use of `pipenv` for managing depedencies and for building the
function when deploying it.

To get setup for developing this project, run:

```bash
make install
```

This Lambda makes use of the `db` module that will be available via a Lambda
Layer once deployed.  For local development purposes, it is installed as an
editable relative `[dev-packages]` dependency

### .env

This Lambda requires a `.env` file in its directory containing the following env
vars:

```plain
PG_PASSWORD="<any-value>"
PG_USER="<any-value>"
PG_DB="<any-value>"
```

This is used whilst running the tests to provide both the Postgres container and
the test code the credentials needed to access the database created.

### Makefile

A `Makefile` is provided to abstract away commonly used commands:

**`make install`**

> This will run `pipenv install --dev` to install development dependencies

**`make lint`**

> This will perform a dry run of `flake8`, `isort`, and `black` and let you know
> what issues were found

**`make format`**

> This will peform a run of `isort` and `black`, this **will** modify files if
> issues were found

**`make test`**

> This will run the unit tests of the project with `pytest` using the contents
> of your `.env` file
