import os

from aws_cdk import aws_ec2, aws_lambda, aws_lambda_python, aws_rds, aws_secretsmanager, aws_sqs, core

REPO_ROOT = os.path.dirname(os.path.abspath(__file__)).replace("app", "")


class DownloaderStack(core.Stack):
    def __init__(
        self,
        scope: core.Construct,
        construct_id: str,
        identifier: str,
        stage: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        vpc = aws_ec2.Vpc(
            self,
            id=f"{construct_id}-vpc",
            cidr="10.0.0.0/24",
        )

        # rds_subnet_group = aws_rds.SubnetGroup(
        #     self,
        #     id=f"{construct_id}-rds-subnet-group",
        #     vpc=vpc,
        #     vpc_subnets=aws_ec2.SubnetSelection(subnet_type=aws_ec2.SubnetType.PRIVATE)
        # )

        # downloader_rds = aws_rds.DatabaseCluster(
        #     self,
        #     id=f"{construct_id}-downloader-rds",
        #     engine=aws_rds.DatabaseClusterEngine.aurora_postgres(
        #         version=aws_rds.AuroraPostgresEngineVersion.VER_11_9
        #     ),
        #     instance_props=aws_rds.InstanceProps(
        #         vpc=vpc,
        #     ),
        #     subnet_group=rds_subnet_group
        # )

        lambda_security_group = aws_ec2.SecurityGroup(
            self,
            id=f"{construct_id}-lambda-security-group",
            vpc=vpc,
            allow_all_outbound=True
        )

        # downloader_rds.connections.allow_default_port_from(
        #     other=lambda_security_group,
        #     description="Allow access from Lambda"
        # )

        aws_lambda.Function(
            self,
            "test",
            code=aws_lambda.Code.from_inline(
                """
import urllib.request as ur
import json

def handler(event, context):
    resp = ur.urlopen('https://jsonplaceholder.typicode.com/todos/1')
    print(json.load(resp))
                """
            ),
            handler="index.handler",
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            memory_size=128,
            timeout=core.Duration.minutes(5),
            vpc=vpc,
            security_group=lambda_security_group
        )

        # to_download_queue = aws_sqs.Queue(
        #     self,
        #     id=f"{construct_id}-to-download-queue",
        #     queue_name="hls-s2-downloader-serverless-to-download",
        #     retention_period=core.Duration.days(14)
        # )

        # psycopg2_layer = aws_lambda.LayerVersion.from_layer_version_arn(
        #     self,
        #     id=f"{construct_id}-pyscopg2-layer",
        #     layer_version_arn=(
        #         "arn:aws:lambda:us-west-2:898466741470:layer:psycopg2-py38:1"
        #     ),
        # )

        # db_layer = aws_lambda_python.PythonLayerVersion(
        #     self,
        #     id=f"{construct_id}-db-layer",
        #     entry=os.path.join(REPO_ROOT, "layers", "db"),
        #     compatible_runtimes=[aws_lambda.Runtime.PYTHON_3_8],
        # )

        # _ = aws_lambda_python.PythonFunction(
        #     self,
        #     id=f"{construct_id}-link-fetcher",
        #     entry=os.path.join(REPO_ROOT, "lambdas", "link_fetcher"),
        #     index="handler.py",
        #     handler="handler",
        #     layers=[db_layer, psycopg2_layer],
        #     memory_size=1024,
        #     timeout=core.Duration.minutes(5),
        #     runtime=aws_lambda.Runtime.PYTHON_3_8,
        #     environment={
        #         "STAGE": stage,
        #         "TO_DOWNLOAD_SQS_QUEUE_URL": to_download_queue.queue_url,
        #     },
        #     vpc=vpc
        # )
