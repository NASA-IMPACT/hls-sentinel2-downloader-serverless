.PHONEY: lint format diff deploy destroy unit-tests

lint:
	pipenv run flake8 lambdas/ app/ tests/ db/ alembic/ app.py
	pipenv run isort --check-only --profile black lambdas/ app/ tests/ db/ alembic/ app.py
	pipenv run black --check --diff lambdas/ app/ tests/ db/ alembic/ app.py

format:
	pipenv run isort --profile black lambdas/ app/ tests/ db/ alembic/ app.py
	pipenv run black lambdas/ app/ tests/ db/ alembic/ app.py

diff:
	pipenv run npx cdk diff || true

deploy:
	pipenv run npx cdk deploy --require-approval never

destroy:
	pipenv run npx cdk destroy --force

unit-tests:
	pipenv run pytest -s tests/unit
