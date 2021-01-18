# HLS Sentinel 2 Downloader Serverless

This project aims to provide a serverless implementation of the current [HLS S2 Downloader](https://github.com/NASA-IMPACT/hls-sentinel2-downloader/tree/version2-ajinkya). The following diagram indicates a high level design for the proposed architecture:

![Diagram of the proposed HLS Sentinel 2 Downloader serverless implementation](./images/hls-s2-downloader.png)

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

To get setup for development, ensure you've installed all the above [requirements](#Requirements), run the following commands in the root of the repository and you'll be good to go!

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
STAGE="<a value for the stage you're deploying to, e.g. $IDENTIFIER, DEV, PRODUCTION>"
AWS_DEFAULT_PROFILE="<your named AWS CLI profile to use for deployment>"
```

An example that you can modify and rename to `.env` is provided: `example.env`

## Useful commands

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

**`make unit-tests`**

> This will run the unit tests of the project with `pytest` using the contents of your `.env` file
