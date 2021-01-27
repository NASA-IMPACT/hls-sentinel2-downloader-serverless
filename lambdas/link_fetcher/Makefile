.PHONEY: lint format

lint:
	pipenv run flake8 .
	pipenv run isort --check-only --profile black .
	pipenv run black --check --diff .

format:
	pipenv run isort --profile black .
	pipenv run black .
