import logging
import os

import boto3
import wget
import pandas as pd
from botocore.exceptions import ClientError

from boto3.s3.transfer import TransferConfig
from fastparquet import write
import re


LOGGING_FORMAT = "%(asctime)s %(name)-12s: %(levelname)-8s -  %(message)s"


class S3Parquetifier:
    def __init__(self, target_bucket=None, source_bucket=None, region="us-east-1", verbose=True):
        """
        Make sure you have the proper credentials to access the AWS buckets and Glue
        :param source_bucket: string, the bucket where the files are
        :param target_bucket: string, the bucket for the parquet files
        :param verbose: boolean, set this to true to see logs
        """

        s3 = boto3.resource("s3", region_name=region)

        if source_bucket:
            try:
                s3.meta.client.head_bucket(Bucket=source_bucket)
            except ClientError as e:
                raise ValueError(f"Bucket {source_bucket} not found or Access denied.")

        if target_bucket:
            try:
                s3.meta.client.head_bucket(Bucket=target_bucket)
            except ClientError as e:
                raise ValueError(f"Bucket {target_bucket} not found or Access denied.")

        self.source_bucket = source_bucket
        self.target_bucket = target_bucket
        self.region = region

        self.logger = logging.getLogger("acropolis.s3Parquetifier")
        self.logger.propagate = verbose

    def convert_from_s3(
        self,
        source_key,
        target_key,
        file_type="csv",
        chunk_size=100000,
        dtype=None,
        skip_rows=None,
        encoding="latin",
        pre_process_chunk=None,
        kwargs={},
    ):
        """
        Convert a file or a series or files from a bucket to another. Can also accept custom functions for the
        transformation of each chunk.
        :param source_key: string, the key of the S3 bucket, it can be a folder from S3 also
        :param target_key: string, the key where the file will be uploaded to, it can be a folder from S3 also
        :param file_type: string, the type of data to process
        :param chunk_size: integer the number of rows per chunk
        :param dtype: dict, the definitions of the csv columns
        :param skip_rows: integer, the number of columns to skip on the CSV
                                                    if False it will generate a uuid for the file name
        :param encoding: string, the encoding of the chunk
        :param pre_process_chunk: function, the function that holds the preprocess method for the chunk
        :param kwargs: dict, the arguments for the preprocess function
        """

        output_file_name = None

        # From the key get all available objects in a list
        s3 = boto3.client("s3", region_name=self.region)

        response = s3.list_objects_v2(Bucket=self.source_bucket, Prefix=source_key)

        list_of_objects = []
        if response["KeyCount"] > 0:
            for file_obj in response["Contents"]:
                if not file_obj["Key"].endswith("/"):
                    list_of_objects.append(file_obj["Key"])

        self.logger.info(
            f"Found {len(list_of_objects)} objects in the path s3://{self.source_bucket}/{source_key}"
        )

        # For each object download it and convert it to parquet
        for file_object in list_of_objects:

            # Get the file name from the object
            output_file_name = os.path.basename(file_object)

            # Download the file from S3
            s3_resource = boto3.resource("s3", region_name=self.region)

            try:
                self.logger.info(f"Downloading file {output_file_name}")
                s3_resource.Bucket(self.source_bucket).download_file(file_object, output_file_name)
                self.logger.info(f"Download completed")
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":  # File not found
                    self.logger.error(
                        f"The object: {file_object} does not exist in bucket: {self.source_bucket}."
                    )
                    Exception("The object does not exist.")
                else:  # Catch general code errors and report
                    self.logger.error(f"Unexpected error: {e}")
                    Exception(f"Unexpected error: {e}")

            try:
                self.logger.info(f"Cleansing bucket: {self.target_bucket}, key: {target_key}")

            except ClientError as e:
                if e.response["Error"]["Code"] == "404":  # File not found
                    self.logger.error("The object does not exist.")
                    Exception("The object does not exist.")
                else:  # Catch general code errors and report
                    self.logger.error(f"Unexpected error: {e}")
                    Exception(f"Unexpected error: {e}")

            try:
                self.convert_from_local(
                    file_name=output_file_name,
                    target_key=target_key,
                    file_type=file_type,
                    chunk_size=chunk_size,
                    dtype=dtype,
                    skip_rows=skip_rows,
                    encoding=None,
                    pre_process_chunk=pre_process_chunk,
                    kwargs=kwargs,
                )
            except UnicodeDecodeError:
                self.convert_from_local(
                    file_name=output_file_name,
                    target_key=target_key,
                    file_type=file_type,
                    chunk_size=chunk_size,
                    dtype=dtype,
                    skip_rows=skip_rows,
                    encoding=encoding,
                    pre_process_chunk=pre_process_chunk,
                    kwargs=kwargs,
                )

        self.logger.info("Done.")

    def convert_from_local(
        self,
        file_name,
        target_key,
        file_type="csv",
        chunk_size=100000,
        dtype=None,
        skip_rows=None,
        encoding=None,
        pre_process_chunk=None,
        kwargs={},
        error_bad_lines=True,
    ):

        """
        Convert files to Parquet locally and upload them to a specific key to S3
        :param file_name: string, the name of the file locally
        :param target_key: string, the key where the file will be uploaded to, it can be a folder from S3 also
        :param file_type: string, the type of data to process
        :param chunk_size: integer the number of rows per chunk
        :param dtype: dict, the definitions of the csv columns
        :param skip_rows: integer, the number of columns to skip on the CSV
        :param encoding: string, the encoding of the chunk
        :param pre_process_chunk: function, the function that holds the preprocess method for the chunk
        :param kwargs: dict, the arguments for the preprocess function
        """

        self.logger.info("Initiating splitting...")
        part_iterator = self._parquetify(
            file_type=file_type,
            file_name=file_name,
            dtype=dtype,
            chunksize=chunk_size,
            skip_rows=skip_rows,
            encoding=encoding,
            pre_process_chunk=pre_process_chunk,
            kwargs=kwargs,
            error_bad_lines=error_bad_lines,
        )

        for part in part_iterator:

            # if the key does not ends in `/` then concat a `/` after the key
            target_path = target_key + part
            if target_key[-1] != "/":
                target_path = target_key + "/" + part

            self.logger.info(f"Created parqet files at {part}")

            self.upload_file_to_s3(part, self.target_bucket, target_path)

        os.unlink(os.path.join(os.getcwd(), file_name))

        self.logger.info(f"Done splitting file {file_name}")

    def delete_prefix(self, bucket, s3_key):
        s3 = boto3.resource("s3", region_name=self.region)
        bucket = s3.Bucket(bucket)
        bucket.objects.filter(Prefix=s3_key).delete()

    def upload_file_to_s3(self, part, bucket_name, target_path):
        s3 = boto3.client("s3", region_name=self.region)

        # Upload file to S3
        config = TransferConfig(
            multipart_threshold=1024 * 25,
            max_concurrency=10,
            multipart_chunksize=1024 * 25,
            use_threads=True,
        )

        s3.upload_file(part, bucket_name, target_path, Config=config)
        self.logger.info(f"Part {part} uploaded to s3://{bucket_name}/{target_path}")

        os.unlink(part)

    def _parquetify(
        self,
        file_name,
        file_type="csv",
        chunksize=1000000,
        skip_rows=None,
        dtype=None,
        encoding=None,
        pre_process_chunk=None,
        kwargs={},
        error_bad_lines=True,
    ):
        """
        Given a file split it into segments and upload it to S3 in a parquet format.
        :param file_name: string, the name of the file locally or a url of the file
        :param skip_rows: integer, how many rows should skip from the csv
        :param dtype: dict, the type of each column in the csv
        """

        file_number = 1

        for chunk in pd.read_csv(
            file_name,
            skiprows=skip_rows,
            chunksize=chunksize,
            encoding=encoding,
            low_memory=True,
            error_bad_lines=error_bad_lines,
        ):

            # apply any pre-processing in the chunk
            object_encoding = "infer" if not dtype else dtype
            if pre_process_chunk:
                chunk = pre_process_chunk(chunk=chunk, **kwargs)

            # construct the name of the part
            chunk_name = f"{file_name.split('.' + file_type)[0].split('/')[-1]}_part_{str(file_number).zfill(4)}.parquet.snappy"

            # export the part as parquet and compressed if needed
            # chunk.to_parquet(chunk_name, compression="snappy", engine="fastparquet")

            write(
                chunk_name,
                chunk,
                has_nulls=True,
                compression="SNAPPY",
                file_scheme="simple",
                object_encoding=object_encoding,
            )

            # yield the generated part for upload
            yield chunk_name
            file_number += 1

        return
