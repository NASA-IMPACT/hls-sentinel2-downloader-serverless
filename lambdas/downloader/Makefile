.PHONEY: clean install lint format test

clean:
	pipenv --rm || true

install:
	pipenv install --dev

lint:
	pipenv run ruff format --diff
	pipenv run ruff check

format:
	pipenv run ruff check --fix
	pipenv run ruff check --select I --fix
	pipenv run ruff format

test:
	pipenv run pytest -v --cov=handler --cov-report term-missing tests/
