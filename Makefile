.PHONEY: install lint format diff deploy destroy diff-integration deploy-integration destroy-integration unit-tests integration-tests

install:
	pipenv install --dev
	$(MAKE) -C lambdas/link_fetcher install
	$(MAKE) -C lambdas/date_generator install
	$(MAKE) -C lambdas/downloader install
	$(MAKE) -C lambdas/mock_scihub_search_api install
	$(MAKE) -C lambdas/mock_scihub_product_api install
	$(MAKE) -C layers/db install
	$(MAKE) -C alembic_migration install

lint:
	pipenv run flake8 cdk/ integration_tests/
	pipenv run isort --check-only --profile black cdk/ integration_tests/
	pipenv run black --check --diff cdk/ integration_tests/
	$(MAKE) -C lambdas/link_fetcher lint
	$(MAKE) -C lambdas/date_generator lint
	$(MAKE) -C lambdas/downloader lint
	$(MAKE) -C lambdas/mock_scihub_search_api lint
	$(MAKE) -C lambdas/mock_scihub_product_api lint
	$(MAKE) -C layers/db lint
	$(MAKE) -C alembic_migration lint

format:
	pipenv run isort --profile black cdk/ integration_tests/
	pipenv run black cdk/ integration_tests/
	$(MAKE) -C lambdas/link_fetcher format
	$(MAKE) -C lambdas/date_generator format
	$(MAKE) -C lambdas/downloader format
	$(MAKE) -C lambdas/mock_scihub_search_api format
	$(MAKE) -C lambdas/mock_scihub_product_api format
	$(MAKE) -C layers/db format
	$(MAKE) -C alembic_migration format

diff:
	pipenv run npx cdk diff --app cdk/app.py || true

deploy:
	pipenv run npx cdk deploy --app cdk/app.py --require-approval never

destroy:
	pipenv run npx cdk destroy --app cdk/app.py --force

diff-integration:
	pipenv run npx cdk diff '*' --app cdk/app_integration.py || true

deploy-integration:
	pipenv run npx cdk deploy '*' --app cdk/app_integration.py --require-approval never

destroy-integration:
	pipenv run npx cdk destroy '*' --app cdk/app_integration.py --force

unit-tests:
	# $(MAKE) -C lambdas/link_fetcher test
	# $(MAKE) -C lambdas/date_generator test
	$(MAKE) -C lambdas/downloader test
	# $(MAKE) -C lambdas/mock_scihub_search_api test
	# $(MAKE) -C lambdas/mock_scihub_product_api test
	# $(MAKE) -C layers/db test
	# $(MAKE) -C alembic_migration test

integration-tests:
	pipenv run pytest -s integration_tests
