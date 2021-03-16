# DB 🗂

## High level overview

The DB layers purpose is to provide `SQLAlchemy` ORM model representations of our database tables and session handling logic to our Lambdas.

To use this layers code in your Lambdas:

```python
from db.session import get_session, get_session_maker
from db.models import Granule

"""
Get a session to your DB (You need the ENV var DB_CONNECTION_SECRET_ARN which points to a AWS Secrets Manager Secret object containing the keys 'username', 'password', 'dbname', and 'host')
"""
session_maker = get_session_maker()
with get_session(session_maker) as db:

    # Query for all granules in our database
    all_granules = db.query(Granule).all()

    # Update a granule
    granule_to_update = db.query(Granule).filter(Granule.id == "an-id").first()
    granule_to_update.size = 1000
    db.commit()
```

---

## Development

This Layer makes use of `pipenv` for managing depedencies and for building the function when deploying it.

To get setup for developing this project, run:

```bash
$ pipenv install --dev
```

_**Note** if you don't have `PIPENV_NO_INHERIT=TRUE` in your env vars, you will need to prepend the above command with it, to make sure you create a `pipenv` `venv` for just this directory._

---

### .env

This Layer requires a `.env` file in its directory containing the following env vars:

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


