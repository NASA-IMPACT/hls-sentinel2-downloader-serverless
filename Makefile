SHELL := $(shell which bash)

.PHONEY:
	clean
	deploy
	deploy-integration
	destroy
	destroy-integration
	diff
	diff-integration
	format
	install
	integration-tests
	lint
	unit-tests

clean:
	pipenv --rm || true
	$(MAKE) -C layers/db clean
	$(MAKE) -C lambdas/link_fetcher clean
	$(MAKE) -C lambdas/date_generator clean
	$(MAKE) -C lambdas/downloader clean
	$(MAKE) -C lambdas/mock_scihub_search_api clean
	$(MAKE) -C lambdas/mock_scihub_product_api clean
	$(MAKE) -C lambdas/requeuer clean
	$(MAKE) -C alembic_migration clean

install:
	pipenv install --dev
	$(MAKE) -C alembic_migration install
	$(MAKE) -C lambdas/date_generator install
	$(MAKE) -C lambdas/link_fetcher install
	$(MAKE) -C lambdas/downloader install
	$(MAKE) -C lambdas/mock_scihub_product_api install
	$(MAKE) -C lambdas/mock_scihub_search_api install
	$(MAKE) -C lambdas/requeuer install
	$(MAKE) -C layers/db install

lint:
	pipenv run ruff format --diff cdk/ integration_tests/
	pipenv run ruff check cdk/ integration_tests/
	$(MAKE) -C lambdas/link_fetcher lint
	$(MAKE) -C lambdas/date_generator lint
	$(MAKE) -C lambdas/downloader lint
	$(MAKE) -C lambdas/mock_scihub_search_api lint
	$(MAKE) -C lambdas/mock_scihub_product_api lint
	$(MAKE) -C lambdas/requeuer lint
	$(MAKE) -C layers/db lint
	$(MAKE) -C alembic_migration lint

format:
	pipenv run ruff format cdk/ integration_tests/
	$(MAKE) -C lambdas/link_fetcher format
	$(MAKE) -C lambdas/date_generator format
	$(MAKE) -C lambdas/downloader format
	$(MAKE) -C lambdas/mock_scihub_search_api format
	$(MAKE) -C lambdas/mock_scihub_product_api format
	$(MAKE) -C lambdas/requeuer format
	$(MAKE) -C layers/db format
	$(MAKE) -C alembic_migration format

diff:
	tox -e dev -- diff --app cdk/app.py || true

deploy:
	tox -e dev -- deploy --app cdk/app.py --require-approval never

destroy:
	tox -e dev -- destroy --app cdk/app.py --force

diff-integration:
	tox -e dev -- diff '*' --app cdk/app_integration.py || true

deploy-integration:
	tox -e dev -- deploy '*' --app cdk/app_integration.py --require-approval never

destroy-integration:
	tox -e dev -- destroy '*' --app cdk/app_integration.py --force

unit-tests:
	$(MAKE) -C lambdas/link_fetcher test
	$(MAKE) -C lambdas/date_generator test
	$(MAKE) -C lambdas/downloader test
	$(MAKE) -C lambdas/mock_scihub_search_api test
	$(MAKE) -C lambdas/mock_scihub_product_api test
	$(MAKE) -C lambdas/requeuer test
	$(MAKE) -C layers/db test
	$(MAKE) -C alembic_migration test

integration-tests:
	tox -e integration

#-------------------------------------------------------------------------------
# For invocation via tox only (i.e., in tox.ini commands entries)
#-------------------------------------------------------------------------------

tox:
	@if [[ -z $${TOX_ENV_DIR+x} ]]; then echo "ERROR: For tox.ini use only" >&2; exit 1; fi

# Install node in the virtualenv, if it's not installed.
install-node: tox
	if [[ ! $$(type node 2>/dev/null) =~ $${VIRTUAL_ENV} ]]; then \
	    nodeenv --node lts --python-virtualenv; \
	fi

# Install cdk in the virtualenv, if it's not installed.
install-cdk: install-node
	if [[ ! $$(type cdk 2>/dev/null) =~ $${VIRTUAL_ENV} ]]; then \
	    npm install --location global "aws-cdk@latest"; \
	fi
