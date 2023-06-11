from setuptools import find_packages, setup

# Runtime requirements.
aws_cdk_version = "1.203.0"
aws_cdk_reqs = [
    "core",
    "aws-lambda",
    "aws-lambda-python",
    "aws-secretsmanager",
    "aws-sqs",
    "aws-ec2",
    "aws-rds",
    "aws-stepfunctions",
    "aws-stepfunctions-tasks",
    "aws-events",
    "aws-events-targets",
    "aws-apigateway",
    "aws-ssm",
    "aws-s3",
    "aws-lambda-event-sources",
    "aws-logs",
]

inst_reqs = [
    "boto3",
    "psycopg2",
    "polling2",
    "python-dotenv",
]

inst_reqs.append([f"aws_cdk.{x}=={aws_cdk_version}" for x in aws_cdk_reqs])

extra_reqs = {
    "test": [
        "assertpy",
        "pytest",
        "pytest-cov",
        "black",
        "flake8",
        "isort",
    ],
    "dev": [
        "assertpy",
        "pytest",
        "black",
        "flake8",
        "nodeenv",
        "isort",
        "pre-commit",
        "pre-commit-hooks",
    ],
}

setup(
    name="hls-sentinel2-downloader-serverless",
    version="0.0.1",
    python_requires=">=3.8",
    author="Development Seed",
    packages=find_packages(),
    package_data={
        ".": [
            "cdk.json",
        ],
    },
    install_requires=inst_reqs,
    extras_require=extra_reqs,
    include_package_data=True,
)
