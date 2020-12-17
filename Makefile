.PHONEY: lint format diff deploy destroy

lint:
	pipenv run flake8 .
	pipenv run isort --check-only --profile black .
	pipenv run black --check --diff .

format:
	pipenv run isort --profile black .
	pipenv run black .

diff:
	pipenv run cdk diff || true

deploy:
	pipenv run cdk deploy --require-approval never

destroy:
	pipenv run cdk destroy --force
