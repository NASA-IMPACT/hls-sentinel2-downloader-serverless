# Alembic Migration 🗂🚛

## High level overview

The Alembic Migration directories purpose is to provide both `alembic` scripts for migrating our databases on deployment and to provide a handler for a AWS CDK Custom Resource which runs our `alembic` scripts upon deployment.

---

## Handler breakdown

Provided below is some pseudo-code to explain the process happening each time the lambda is invoked:

```python
if cdk_delete:
    tell_cloud_formation_everything_is_fine()

try:
    get_a_connection_to_rds()
    setup_alembic_config()
    run_alembic_scripts()
    tell_cloud_formation_everything_is_fine()
except:
    tell_cloud_formation_something_went_wrong()
```

---

## Development

This directory makes use of `pipenv` for managing depedencies and for building the function when deploying it.

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

**`make lint`**

> This will perform a dry run of `flake8`, `isort`, and `black` and let you know what issues were found

**`make format`**

> This will peform a run of `isort` and `black`, this **will** modify files if issues were found

**`make test`**

> This will run the unit tests of the project with `pytest` using the contents of your `.env` file

---
