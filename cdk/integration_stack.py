import json

from aws_cdk import (
    aws_apigateway,
    aws_lambda,
    aws_lambda_python,
    aws_secretsmanager,
    core,
)


class IntegrationStack(core.Stack):
    def __init__(
        self,
        scope: core.Construct,
        construct_id: str,
        identifier: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        aws_secretsmanager.Secret(
            self,
            id=f"{identifier}-integration-scihub-credentials",
            secret_name=f"hls-s2-downloader-serverless/{identifier}/scihub-credentials",
            description="Dummy values for the Mock SciHub API credentials",
            generate_secret_string=aws_secretsmanager.SecretStringGenerator(
                secret_string_template=json.dumps({"username": "test-user"}),
                generate_string_key="password",
            ),
        )

        mock_scihub_api_lambda = aws_lambda_python.PythonFunction(
            self,
            id=f"{identifier}-mock-scihub-api-lambda",
            entry="lambdas/mock_scihub_api",
            index="handler.py",
            handler="handler",
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            timeout=core.Duration.minutes(1),
            memory_size=128,
        )

        mock_scihub_api = aws_apigateway.RestApi(
            self,
            id=f"{identifier}-mock-scihub-api",
        )

        self.scihub_url = mock_scihub_api.url.rsplit("/", 1)[0]

        aws_apigateway.Resource(
            self,
            id=f"{identifier}-mock-scihub-api-dhus-search",
            parent=mock_scihub_api.root,
            path_part="dhus",
        ).add_resource("search").add_method(
            http_method="GET",
            method_responses=[
                aws_apigateway.MethodResponse(
                    status_code="200",
                    response_models={
                        "application/json": aws_apigateway.Model.EMPTY_MODEL
                    },
                )
            ],
            integration=aws_apigateway.LambdaIntegration(
                handler=mock_scihub_api_lambda,
                integration_responses=[
                    aws_apigateway.IntegrationResponse(status_code="200")
                ],
            ),
        )
