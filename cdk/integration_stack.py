import json
from typing import Optional

from aws_cdk import Duration, RemovalPolicy, Stack, aws_apigateway, aws_iam, aws_lambda
from aws_cdk import aws_lambda_python_alpha as aws_lambda_python
from aws_cdk import aws_logs, aws_s3, aws_secretsmanager, aws_ssm
from constructs import Construct


class IntegrationStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        identifier: str,
        permissions_boundary_arn: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        if permissions_boundary_arn:
            aws_iam.PermissionsBoundary.of(self).apply(
                aws_iam.ManagedPolicy.from_managed_policy_arn(
                    self,
                    "PermissionsBoundary",
                    permissions_boundary_arn,
                )
            )

        aws_secretsmanager.Secret(
            self,
            id=f"{identifier}-integration-esa-subscription-credentials",
            secret_name=f"hls-s2-downloader-serverless/{identifier}/esa-subscription-credentials",
            description="Dummy values for the ESA 'push' subscription authentication",
            generate_secret_string=aws_secretsmanager.SecretStringGenerator(
                secret_string_template=json.dumps({"notification_username": "test-user"}),
                generate_string_key="notification_password",
            ),
        )

        mock_scihub_search_api_lambda = aws_lambda_python.PythonFunction(
            self,
            id=f"{identifier}-mock-scihub-api-lambda",
            entry="lambdas/mock_scihub_search_api",
            index="handler.py",
            handler="handler",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            timeout=Duration.minutes(1),
            memory_size=128,
        )

        aws_logs.LogGroup(
            self,
            id=f"{identifier}-mock-scihub-search-api-log-group",
            log_group_name=f"/aws/lambda/{mock_scihub_search_api_lambda.function_name}",
            removal_policy=RemovalPolicy.DESTROY,
            retention=aws_logs.RetentionDays.ONE_DAY,
        )

        mock_scihub_product_api_lambda = aws_lambda_python.PythonFunction(
            self,
            id=f"{identifier}-mock-scihub-product-lambda",
            entry="lambdas/mock_scihub_product_api",
            index="handler.py",
            handler="handler",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            timeout=Duration.minutes(1),
            memory_size=128,
        )

        aws_logs.LogGroup(
            self,
            id=f"{identifier}-mock-scihub-product-api-log-group",
            log_group_name=(
                f"/aws/lambda/{mock_scihub_product_api_lambda.function_name}"
            ),
            removal_policy=RemovalPolicy.DESTROY,
            retention=aws_logs.RetentionDays.ONE_DAY,
        )

        mock_scihub_api = aws_apigateway.RestApi(
            self, id=f"{identifier}-mock-scihub-api", binary_media_types=["*/*"]
        )

        self.scihub_url = mock_scihub_api.url.rsplit("/", 1)[0]

        aws_ssm.StringParameter(
            self,
            id=f"{identifier}-mock-scihub-url",
            string_value=self.scihub_url,
            parameter_name=f"/integration_tests/{identifier}/mock_scihub_url",
        )

        search_resource = aws_apigateway.Resource(
            self,
            id=f"{identifier}-mock-scihub-api-search-search",
            parent=mock_scihub_api.root,
            path_part="resto",
        )

        search_resource.add_resource("api").add_resource("collections").add_resource(
            "Sentinel2"
        ).add_resource("search.json").add_method(
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
                handler=mock_scihub_search_api_lambda,
                integration_responses=[
                    aws_apigateway.IntegrationResponse(status_code="200")
                ],
            ),
        )

        download_resource = aws_apigateway.Resource(
            self,
            id=f"{identifier}-mock-scihub-api-download-search",
            parent=mock_scihub_api.root,
            path_part="odata",
        )

        download_resource.add_resource("v1").add_resource("{product+}").add_method(
            http_method="GET",
            method_responses=[
                aws_apigateway.MethodResponse(
                    status_code="200",
                    response_models={
                        "application/octect-stream": aws_apigateway.Model.EMPTY_MODEL,
                        "application/json": aws_apigateway.Model.EMPTY_MODEL,
                    },
                )
            ],
            integration=aws_apigateway.LambdaIntegration(
                handler=mock_scihub_product_api_lambda,
                integration_responses=[
                    aws_apigateway.IntegrationResponse(status_code="200")
                ],
                content_handling=aws_apigateway.ContentHandling.CONVERT_TO_BINARY,
            ),
        )

        self.upload_bucket = aws_s3.Bucket(
            self,
            id=f"{identifier}-upload-bucket",
            access_control=aws_s3.BucketAccessControl.PRIVATE,
            removal_policy=RemovalPolicy.DESTROY,
        )

        aws_ssm.StringParameter(
            self,
            id=f"{identifier}-upload-bucket-name",
            string_value=self.upload_bucket.bucket_name,
            parameter_name=f"/integration_tests/{identifier}/upload_bucket_name",
        )
