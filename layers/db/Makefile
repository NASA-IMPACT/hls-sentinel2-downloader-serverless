.PHONEY: install lint format test

install:
	pipenv install --dev

lint:
	pipenv run flake8 .
	pipenv run isort --check-only --profile black .
	pipenv run black --check --diff .

format:
	pipenv run isort --profile black .
	pipenv run black .

test:
	pipenv run pytest -s --cov=db --cov-report term-missing db/tests/
