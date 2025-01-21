from aws_cdk import aws_lambda_python_alpha as aws_lambda_python

DEFAULT_BUNDLING_OPTIONS = aws_lambda_python.BundlingOptions(
    asset_excludes=[
        "__pycache__/",
        ".mypy_cache/",
        ".ruff_cache/",
        ".vscode/",
        "tests/",
    ]
)
