from setuptools import find_packages, setup

# Runtime requirements.
inst_reqs = [
    "aws-cdk-lib>=2.0.0",
    "aws-cdk.aws-lambda-python-alpha",
    "constructs>=10.0.0",
    "boto3",
    "polling2",
    "psycopg2-binary==2.9.10",
    "python-dotenv",
]

boto_stubs = (
    "boto3-stubs[lambda,s3,sqs,ssm,stepfunctions]",
    "botocore-stubs",
)

extra_reqs = {
    "test": [
        "assertpy",
        "black",
        *boto_stubs,
        "flake8",
        "isort",
        "moto",
        "mypy",
        "pytest",
        "pytest-cov",
        "requests",
    ],
    "dev": [
        "assertpy",
        "black",
        *boto_stubs,
        "flake8",
        "isort",
        "mypy",
        "nodeenv",
        "pre-commit",
        "pre-commit-hooks",
        "pytest",
    ],
}

setup(
    name="hls-sentinel2-downloader-serverless",
    version="0.0.1",
    python_requires=">=3.11",
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
