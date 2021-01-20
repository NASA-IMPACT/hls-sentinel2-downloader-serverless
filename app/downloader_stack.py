import os
from shutil import copytree, move, rmtree
from typing import List

from aws_cdk import aws_lambda, aws_lambda_python, core

REPO_BASE_PATH = os.path.dirname(os.path.abspath(__file__)).replace(
    f"{str(os.path.sep)}app", ""
)


class DownloaderStack(core.Stack):
    def __init__(
        self, scope: core.Construct, construct_id: str, identifier: str, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        psycopg2_layer = aws_lambda.LayerVersion.from_layer_version_arn(
            self,
            id="pyscopg2-layer",
            layer_version_arn=(
                "arn:aws:lambda:us-west-2:898466741470:layer:psycopg2-py38:1"
            ),
        )

        link_fetcher = self.create_function_with_local_dependencies(
            lambda_name="link_fetcher",
            local_module_names=["db"],
            layers=[psycopg2_layer],
            duration=core.Duration.minutes(5),
            memory_size=1024
        )

    def create_function_with_local_dependencies(
        self,
        lambda_name: str,
        local_module_names: List[str],
        layers: List[aws_lambda.ILayerVersion],
        duration: core.Duration,
        memory_size: int,
    ) -> aws_lambda_python.PythonFunction:
        # Prepare build directory (Remove previous runs and re-create it)
        build_dir = os.path.join(REPO_BASE_PATH, ".build", lambda_name)
        rmtree(build_dir)
        os.makedirs(build_dir)

        # Copy lambda src to build dir
        copytree(
            src=os.path.join(REPO_BASE_PATH, "lambdas", lambda_name),
            dst=os.path.join(build_dir, "lambdas", lambda_name),
            dirs_exist_ok=True,
        )

        # If we use modules within the repo, copy those to beside the lambda src
        for module_name in local_module_names:
            copytree(
                src=os.path.join(REPO_BASE_PATH, module_name),
                dst=os.path.join(build_dir, module_name),
                dirs_exist_ok=True,
            )

        # aws-lambda-python uses Pipfile as the root for the docker context, move this
        # alongside the modules and lambda src so everything is included
        move(
            src=os.path.join(build_dir, "lambdas", lambda_name, "Pipfile"),
            dst=os.path.join(build_dir, "Pipfile"),
        )

        return aws_lambda_python.PythonFunction(
            self,
            id=lambda_name,
            entry=build_dir,
            handler="handler",
            index=os.path.join("lambdas", lambda_name, "handler.py"),
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            memory_size=memory_size,
            timeout=duration,
            layers=layers,
        )
