.PHONEY: lint format diff deploy destroy unit-tests

lint:
	pipenv run flake8 lambdas/ cdk/ layers/ alembic_migration/
	pipenv run isort --check-only --profile black lambdas/ cdk/ layers/ alembic_migration/
	pipenv run black --check --diff lambdas/ cdk/ layers/ alembic_migration/

format:
	pipenv run isort --profile black lambdas/ cdk/ layers/ alembic_migration/
	pipenv run black lambdas/ cdk/ layers/ alembic_migration/

diff:
	pipenv run npx cdk diff --app cdk/app.py || true

deploy:
	pipenv run npx cdk deploy --app cdk/app.py --require-approval never

destroy:
	pipenv run npx cdk destroy --app cdk/app.py --force

unit-tests:
	$(MAKE) -C lambdas/link_fetcher test
	$(MAKE) -C layers/db test
	$(MAKE) -C alembic_migration test
