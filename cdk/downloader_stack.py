from aws_cdk import (
    aws_cloudwatch,
    aws_ec2,
    aws_events,
    aws_events_targets,
    aws_iam,
    aws_lambda,
    aws_lambda_event_sources,
    aws_lambda_python,
    aws_logs,
    aws_rds,
    aws_secretsmanager,
    aws_sqs,
    aws_ssm,
    aws_stepfunctions,
    aws_stepfunctions_tasks,
    core,
)


class DownloaderStack(core.Stack):
    def __init__(
        self,
        scope: core.Construct,
        construct_id: str,
        identifier: str,
        upload_bucket: str,
        scihub_url: str = None,
        enable_downloading: bool = False,
        use_inthub2: bool = False,
        schedule_link_fetching: bool = False,
        removal_policy_destroy: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        vpc = aws_ec2.Vpc(
            self,
            id=f"{identifier}-vpc",
            nat_gateways=0,
            enable_dns_hostnames=True,
            enable_dns_support=True,
            cidr="10.0.0.0/26",
            subnet_configuration=[
                aws_ec2.SubnetConfiguration(
                    name="PublicSubnet1", subnet_type=aws_ec2.SubnetType.PUBLIC
                )
            ],
            max_azs=3,
        )

        rds_subnet_group = aws_rds.SubnetGroup(
            self,
            id=f"{identifier}-rds-subnet-group",
            vpc=vpc,
            vpc_subnets=aws_ec2.SubnetSelection(subnet_type=aws_ec2.SubnetType.PUBLIC),
            description=f"Subnet group for {identifier}-downloader-rds",
        )

        rds_security_group = aws_ec2.SecurityGroup(
            self,
            id=f"{identifier}-rds-security-group",
            vpc=vpc,
            allow_all_outbound=True,
            description=f"Security group for {identifier}-downloader-rds",
        )

        rds_security_group.add_ingress_rule(
            peer=aws_ec2.Peer.any_ipv4(),
            connection=aws_ec2.Port.tcp(5432),
            description="Allow all traffic for Postgres",
        )

        downloader_rds = aws_rds.DatabaseCluster(
            self,
            id=f"{identifier}-downloader-rds",
            engine=aws_rds.DatabaseClusterEngine.aurora_postgres(
                version=aws_rds.AuroraPostgresEngineVersion.VER_10_12
            ),
            instance_props=aws_rds.InstanceProps(
                vpc=vpc,
                security_groups=[rds_security_group],
                publicly_accessible=True,
            ),
            subnet_group=rds_subnet_group,
            default_database_name="hlss2downloader",
            removal_policy=core.RemovalPolicy.DESTROY
            if removal_policy_destroy
            else core.RemovalPolicy.RETAIN,
        )

        aws_ssm.StringParameter(
            self,
            id=f"{identifier}-downloader-rds-secret-arn",
            string_value=downloader_rds.secret.secret_arn,
            parameter_name=f"/integration_tests/{identifier}/downloader_rds_secret_arn",
        )

        core.CfnOutput(
            self,
            id=f"{identifier}-downloader-ip",
            value=downloader_rds.cluster_endpoint.hostname,
        )

        psycopg2_layer = aws_lambda.LayerVersion.from_layer_version_arn(
            self,
            id=f"{identifier}-pyscopg2-layer",
            layer_version_arn=(
                "arn:aws:lambda:us-west-2:898466741470:layer:psycopg2-py38:1"
            ),
        )

        db_layer = aws_lambda_python.PythonLayerVersion(
            self,
            id=f"{identifier}-db-layer",
            entry="layers/db",
            compatible_runtimes=[aws_lambda.Runtime.PYTHON_3_8],
        )

        migration_function = aws_lambda_python.PythonFunction(
            self,
            id=f"{identifier}-migration-function",
            entry="alembic_migration",
            handler="handler",
            index="alembic_handler.py",
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            memory_size=128,
            timeout=core.Duration.minutes(5),
            layers=[
                db_layer,
                psycopg2_layer,
            ],
            environment={"DB_CONNECTION_SECRET_ARN": downloader_rds.secret.secret_arn},
        )

        downloader_rds.secret.grant_read(migration_function)

        core.CustomResource(
            self,
            id=f"{identifier}-migration-function-resource",
            service_token=migration_function.function_arn,
        )

        to_download_queue = aws_sqs.Queue(
            self,
            id=f"{identifier}-to-download-queue",
            queue_name=f"hls-s2-downloader-serverless-{identifier}-to-download"[-80:],
            retention_period=core.Duration.days(14),
            visibility_timeout=core.Duration.minutes(15),
        )

        aws_ssm.StringParameter(
            self,
            id=f"{identifier}-to-download-queue-url",
            string_value=to_download_queue.queue_url,
            parameter_name=f"/integration_tests/{identifier}/to_download_queue_url",
        )

        date_generator = aws_lambda_python.PythonFunction(
            self,
            id=f"{identifier}-date-generator",
            entry="lambdas/date_generator",
            index="handler.py",
            handler="handler",
            memory_size=128,
            timeout=core.Duration.seconds(15),
            runtime=aws_lambda.Runtime.PYTHON_3_8,
        )

        aws_logs.LogGroup(
            self,
            id=f"{identifier}-date-generator-log-group",
            log_group_name=f"/aws/lambda/{date_generator.function_name}",
            removal_policy=core.RemovalPolicy.DESTROY
            if removal_policy_destroy
            else core.RemovalPolicy.RETAIN,
            retention=aws_logs.RetentionDays.ONE_DAY
            if removal_policy_destroy
            else aws_logs.RetentionDays.TWO_WEEKS,
        )

        link_fetcher_environment_vars = {
            "STAGE": identifier,
            "TO_DOWNLOAD_SQS_QUEUE_URL": to_download_queue.queue_url,
            "DB_CONNECTION_SECRET_ARN": downloader_rds.secret.secret_arn,
        }

        if scihub_url:
            link_fetcher_environment_vars["SCIHUB_URL"] = scihub_url

        lambda_insights_policy = aws_iam.ManagedPolicy.from_managed_policy_arn(
            self,
            id=f"cloudwatch-lambda-insights-policy-{identifier}",
            managed_policy_arn=(
                "arn:aws:iam::aws:policy/CloudWatchLambdaInsightsExecutionRolePolicy"
            ),
        )

        insights_layer = aws_lambda.LayerVersion.from_layer_version_arn(
            self,
            id=f"lambda-insights-extension-{identifier}",
            layer_version_arn=(
                "arn:aws:lambda:us-west-2:580247275435:"
                "layer:LambdaInsightsExtension:14"
            ),
        )

        link_fetcher = aws_lambda_python.PythonFunction(
            self,
            id=f"{identifier}-link-fetcher",
            entry="lambdas/link_fetcher",
            index="handler.py",
            handler="handler",
            layers=[db_layer, psycopg2_layer],
            memory_size=200,
            timeout=core.Duration.minutes(15),
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            environment=link_fetcher_environment_vars,
        )

        aws_logs.LogGroup(
            self,
            id=f"{identifier}-link-fetcher-log-group",
            log_group_name=f"/aws/lambda/{link_fetcher.function_name}",
            removal_policy=core.RemovalPolicy.DESTROY
            if removal_policy_destroy
            else core.RemovalPolicy.RETAIN,
            retention=aws_logs.RetentionDays.ONE_DAY
            if removal_policy_destroy
            else aws_logs.RetentionDays.TWO_WEEKS,
        )

        aws_cloudwatch.Alarm(
            self,
            id=f"{identifier}-link-fetcher-errors-alarm",
            metric=link_fetcher.metric_errors(),
            evaluation_periods=3,
            threshold=1,
        )

        downloader_environment_vars = {
            "STAGE": identifier,
            "DB_CONNECTION_SECRET_ARN": downloader_rds.secret.secret_arn,
            "UPLOAD_BUCKET": upload_bucket,
            "USE_INTHUB2": "YES" if use_inthub2 else "NO",
        }

        if scihub_url:
            downloader_environment_vars["SCIHUB_URL"] = scihub_url

        self.downloader = aws_lambda_python.PythonFunction(
            self,
            id=f"{identifier}-downloader",
            entry="lambdas/downloader",
            index="handler.py",
            handler="handler",
            layers=[db_layer, psycopg2_layer, insights_layer],
            memory_size=1200,
            timeout=core.Duration.minutes(15),
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            environment=downloader_environment_vars,
            reserved_concurrent_executions=15,
        )

        aws_logs.LogGroup(
            self,
            id=f"{identifier}-downloader-log-group",
            log_group_name=f"/aws/lambda/{self.downloader.function_name}",
            removal_policy=core.RemovalPolicy.DESTROY
            if removal_policy_destroy
            else core.RemovalPolicy.RETAIN,
            retention=aws_logs.RetentionDays.ONE_DAY
            if removal_policy_destroy
            else aws_logs.RetentionDays.TWO_WEEKS,
        )

        aws_cloudwatch.Alarm(
            self,
            id=f"{identifier}-downloader-errors-alarm",
            metric=self.downloader.metric_errors(),
            evaluation_periods=3,
            threshold=1,
        )

        aws_ssm.StringParameter(
            self,
            id=f"{identifier}-downloader-arn",
            string_value=self.downloader.function_arn,
            parameter_name=f"/integration_tests/{identifier}/downloader_arn",
        )

        self.downloader.role.add_managed_policy(lambda_insights_policy)

        downloader_rds.secret.grant_read(link_fetcher)
        downloader_rds.secret.grant_read(self.downloader)

        scihub_credentials = aws_secretsmanager.Secret.from_secret_name_v2(
            self,
            id=f"{identifier}-scihub-credentials",
            secret_name=f"hls-s2-downloader-serverless/{identifier}/scihub-credentials",
        )
        scihub_credentials.grant_read(link_fetcher)
        scihub_credentials.grant_read(self.downloader)

        if use_inthub2:
            inthub2_credentials = aws_secretsmanager.Secret.from_secret_name_v2(
                self,
                id=f"{identifier}-inthub2-credentials",
                secret_name=(
                    f"hls-s2-downloader-serverless/{identifier}/inthub2-credentials"
                ),
            )
            inthub2_credentials.grant_read(self.downloader)

        to_download_queue.grant_send_messages(link_fetcher)
        to_download_queue.grant_consume_messages(self.downloader)
        self.downloader.add_event_source(
            aws_lambda_event_sources.SqsEventSource(
                queue=to_download_queue, batch_size=1, enabled=enable_downloading
            )
        )

        date_generator_task = aws_stepfunctions_tasks.LambdaInvoke(
            self,
            id=f"{identifier}-date-generator-invoke",
            lambda_function=date_generator,
        )

        link_fetcher_task = aws_stepfunctions_tasks.LambdaInvoke(
            self,
            id=f"{identifier}-link-fetcher-invoke",
            lambda_function=link_fetcher,
        )

        link_fetcher_map_task = aws_stepfunctions.Map(
            self,
            id=f"{identifier}-link-fetcher-map",
            input_path="$.Payload.query_dates",
            parameters={"query_date.$": "$$.Map.Item.Value"},
            max_concurrency=3,
        ).iterator(link_fetcher_task)

        link_fetcher_step_function_definition = date_generator_task.next(
            link_fetcher_map_task
        )

        link_fetcher_step_function = aws_stepfunctions.StateMachine(
            self,
            id=f"{identifier}-link-fetcher-step-function",
            definition=link_fetcher_step_function_definition,
        )

        aws_ssm.StringParameter(
            self,
            id=f"{identifier}-link-fetcher-step-function-arn",
            string_value=link_fetcher_step_function.state_machine_arn,
            parameter_name=(
                f"/integration_tests/{identifier}/link_fetcher_step_function_arn"
            ),
        )

        if schedule_link_fetching:
            _ = aws_events.Rule(
                self,
                id=f"{identifier}-link-fetch-rule",
                schedule=aws_events.Schedule.expression("cron(0 12 * * ? *)"),
            ).add_target(aws_events_targets.SfnStateMachine(link_fetcher_step_function))
