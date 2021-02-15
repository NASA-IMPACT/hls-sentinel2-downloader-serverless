# HLS Sentinel 2 Downloader Serverless 🛰

This project aims to provide a serverless implementation of the current [HLS S2 Downloader](https://github.com/NASA-IMPACT/hls-sentinel2-downloader/tree/version2-ajinkya). The following diagram indicates a high level design for the proposed architecture:

![Diagram of the proposed HLS Sentinel 2 Downloader serverless implementation](./images/hls-s2-downloader.png)

# Contents

* [Development - Requirements](#requirements)
* [Development - Getting started 🏃‍♀️](#getting-started-🏃‍♀️)
* [Development - Repository TL;DR:](#repository-tldr)
* [Development - Lambda and Layer development TL;DR:](#lambda-and-layer-development-tldr)
* [Development - Makefile goodness](#makefile-goodness)

# Development

## Requirements

To develop on this project, you should install:

* NVM [Node Version Manager](https://github.com/nvm-sh/nvm) / Node 12
* [AWS CDK](https://docs.aws.amazon.com/cdk/latest/guide/getting_started.html) - There is a `package.json` in the repository, it's recommended to run `npm install` in the repository root and make use of `npx <command>` rather than globally installing AWS CDK
* [pyenv](https://github.com/pyenv/pyenv) / Python 3.8.6
* [pipenv](https://github.com/pypa/pipenv)
* [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html)
* [OpenSSL](https://github.com/openssl/openssl) (For Postgres/DB work)

If you're developing on MacOS, all of the above (apart from AWS CDK) can be installed using [homebrew](https://brew.sh/)

## Getting started 🏃‍♀️

To get setup for overall development, ensure you've installed all the above [requirements](#Requirements), run the following commands in the root of the repository and you'll be good to go!

```bash
$ nvm install # This sets up your node environment
$ npm install # This installs any node packages that are within package.json (CDK etc.)
$ pipenv install --dev # This installs any python packages that are within Pipfile
```

_**Note** you might have an issue installing `psycopg2` - I found [this](https://github.com/pypa/pipenv/issues/3991#issuecomment-564645309) helpful_

A file named `.env` is expected in the root of the repository, the expected values are:

```bash
OWNER="<your name>"
IDENTIFIER="<a unique value to tie to your cdk deployment>"
AWS_DEFAULT_REGION="<the AWS region you're deploying to>"
AWS_DEFAULT_PROFILE="<your named AWS CLI profile to use for deployment>"
PIPENV_NO_INHERIT=TRUE # This is used to ensure our Lambdas/Layers get separate Pipenv environments
```

An example that you can modify and rename to `.env` is provided: `example.env`

## Repository TL;DR:

This project has 5 main directories in which you'll find the majority of code needed for `hls-sentinel2-downloader-serverless`:

```
├── alembic_migration # Specific Alembic code for database migration - Includes code for bootstrapping a DB with CDK
├── cdk # AWS CDK code for deploying both the downloader stack and integration test stacks
├── integration_tests # Pytest integration tests
├── lambdas # Directory of Lambdas and their associated code
└── layers # Directory of Layers (common code modules used across our Lambdas)
```

The pattern for this monorepo approach was devised amongst a few folks at Development Seed, you can read up on this pattern and how it fits together [here at the example repository](https://github.com/alukach/cdk-python-lambda-monorepo).

Most directories will contain a README to explain what the purpose is of the component and how to develop it.

## Lambda and Layer development TL;DR:

Every Lambda and Layer directory has a `Makefile` inside, this contains a subset of the commands found in the [root repository Makefile](#makefile-goodness). Each `Makefile` should come with `lint`, `format`, and `test` as standard, these are then added as calls in the root Makefile so that we can lint/format/unit test all our code at a project level.

Per-Lambda/Layer development is recommended to be done by opening the specific components directory in a new IDE window (this just makes life easier for IDE prompts etc as the directory has its own `Pipenv` virtual environment). To get started, go into the directory of the Lambda/Layer and run:

```
$ pipenv install --dev # Creates a Pipenv env for the directory and installs the dependencies
```

For further guides on how to make new Lambdas/Layers, follow the examples in [the example monorepo repo](https://github.com/alukach/cdk-python-lambda-monorepo).

## Makefile goodness

A `Makefile` is available in the root of the repository to abstract away commonly used commands for development:

**`make lint`**

> This will perform a dry run of `flake8`, `isort`, and `black` and let you know what issues were found

**`make format`**

> This will peform a run of `isort` and `black`, this **will** modify files if issues were found

**`make diff`**

> This will run a `cdk diff` using the contents of your `.env` file

**`make deploy`**

> This will run a `cdk deploy` using the contents of your `.env` file. The deployment is auto-approved, so **make sure** you know what you're changing with your deployment first! (Best to run `make diff` to check!)

**`make destroy`**

> This will run a `cdk destroy` using the contents of your `.env` file. The destroy is auto-approved, so **make sure** you know what you're destroying first!

**`make diff-integration`**

> This will run a `cdk diff` using the contents of your `.env` file on the integration test stack

**`make deploy-integration`**

> This will run a `cdk deploy` using the contents of your `.env` file on the integration test stack. The deployment is auto-approved, so **make sure** you know what you're changing with your deployment first! (Best to run `make diff` to check!)

**`make destroy-integration`**

> This will run a `cdk destroy` using the contents of your `.env` file on the integration test stack. The destroy is auto-approved, so **make sure** you know what you're destroying first!

**`make unit-tests`**

> This will run the unit tests within the project with `pytest`

**`make integration-tests`**

> This will run the integration tests within the project with `pytest` **You need to have run `make deploy-integration` first, otherwise these will fail straight away**
