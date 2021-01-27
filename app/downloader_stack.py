import os

from aws_cdk import aws_lambda, aws_lambda_python, core

REPO_ROOT = os.path.dirname(os.path.abspath(__file__)).replace("app", "")


class DownloaderStack(core.Stack):
    def __init__(
        self, scope: core.Construct, construct_id: str, identifier: str, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        psycopg2_layer = aws_lambda.LayerVersion.from_layer_version_arn(
            self,
            id=f"{construct_id}-pyscopg2-layer",
            layer_version_arn=(
                "arn:aws:lambda:us-west-2:898466741470:layer:psycopg2-py38:1"
            ),
        )

        db_layer = aws_lambda_python.PythonLayerVersion(
            self,
            id=f"{construct_id}-db-layer",
            entry=os.path.join(REPO_ROOT, "layers", "db"),
            compatible_runtimes=[aws_lambda.Runtime.PYTHON_3_8],
        )

        _ = aws_lambda_python.PythonFunction(
            self,
            id=f"{construct_id}-link-fetcher",
            entry=os.path.join(REPO_ROOT, "lambdas", "link_fetcher"),
            index="handler.py",
            handler="handler",
            layers=[db_layer, psycopg2_layer],
            memory_size=1024,
            timeout=core.Duration.minutes(5),
            runtime=aws_lambda.Runtime.PYTHON_3_8,
        )
