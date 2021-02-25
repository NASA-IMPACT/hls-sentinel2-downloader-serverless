import setuptools

setuptools.setup(
    name="db",
    version="0.0.1",
    author="ciaranevans",
    author_email="ciaran@developmentseed.org",
    description="Database models and utilities for the S2 Downloader",
    long_description="Database models and utilities for the S2 Downloader",
    long_description_content_type="text/markdown",
    url="https://github.com/NASA-IMPACT/hls-sentinel2-downloader-serverless",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=setuptools.find_packages(),
    python_requires=">=3.8",
    install_requires=["sqlalchemy>=1.3.22"],
)
