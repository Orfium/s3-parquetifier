from setuptools import setup, find_packages
from distutils.command.install import INSTALL_SCHEMES

for scheme in INSTALL_SCHEMES.values():
    scheme['data'] = scheme['purelib']

try:
    with open("README.md", "r") as fh:
        long_description = fh.read()
except:
    long_description = ''

setup(
    # Application name
    name="s3-parquetifier",

    # Version number
    version="0.1.1",

    # Application author details
    author="Konstantinos Siaterlis",
    author_email="siaterliskonsta@gmail.com",

    # Packages
    packages=find_packages(),

    # Details
    license="LICENSE",
    description="ETL job from CSV to Parquet in AWS S3",
    long_description=long_description,
    long_description_content_type="text/markdown",

    url='https://github.com/Orfium/s3-parquetifier',

    # Dependent packages (distributions)
    install_requires=[
        "boto3",
        "pandas",
        "pyarrow",
        "requests",
        "wget"
    ],
)