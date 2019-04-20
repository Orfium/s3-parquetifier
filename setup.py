from setuptools import setup

setup(
    name='S3-Parquetifier',
    version='0.0.1',
    description='ETL job from CSV to Parquet in AWS S3',

    # Application author details:
    author='Konstantinos Siaterlis',
    author_email="kostas@orfium.com",

    # Packages
    packages=["s3-parquetifier", ],

    # Details
    url="https://github.com/siakon89/s3-parquetifier",

    # Dependent packages (distributions)
    install_requires=[
        "boto3",
        "pandas",
        "pyarrow",
        "requests"
    ],
)