.PHONEY: lint format diff deploy destroy

lint:
	pipenv run flake8 .
	pipenv run isort --check-only --profile black .
	pipenv run black --check --diff .

format:
	pipenv run isort --profile black .
	pipenv run black .

diff:
	pipenv run npx cdk diff || true

deploy:
	pipenv run npx cdk deploy --require-approval never

destroy:
	pipenv run npx cdk destroy --force
