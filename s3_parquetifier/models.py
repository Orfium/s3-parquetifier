import logging
from .utils import logger, Utils
from .aws_client import AWSClient
import os, wget


class S3Parquetifier:

    def __init__(
        self,
        aws_access_key=None,
        aws_secret_key=None,
        region='us-west-2',
        verbose=False,
        source_bucket=None,
        target_bucket=None,
        s3_enabled=True
    ):
        """
        Make sure you have the proper credentials to access the AWS buckets and Glue
        :param aws_access_key: the AWS access key from your AWS Account
        :param aws_secret_key: the AWS secret key from your AWS Account
        :param verbose: boolean, set this to true to see logs
        :param source_bucket: string, the bucket where the files are
        :param target_bucket: string, the bucket for the parquet files
        :param s3_enabled: Boolean, True  : the init needs the AWS Credentials
                                    False : can be used for local operations
        """

        self.aws_client = None

        if s3_enabled:

            self.aws_access_key = aws_access_key
            self.aws_secret_key = aws_secret_key
            self.region = region

            if not source_bucket or not target_bucket:
                raise ValueError('Please provide a source and a target bucket.')

            # Check if the buckets exists or the client has access
            self.aws_client = AWSClient(secret_key=aws_secret_key, access_key=aws_access_key, region=region)

            if not self.aws_client.check_if_bucket_exists(bucket=source_bucket):
                raise ValueError('Bucket {} not found or Access denied.'.format(source_bucket))

            if not self.aws_client.check_if_bucket_exists(bucket=target_bucket):
                raise ValueError('Bucket {} not found or Access denied.'.format(target_bucket))

            self.source_bucket = source_bucket
            self.target_bucket = target_bucket

        logging.getLogger('siakon.s3Parquetifier').propagate = verbose

    def convert_from_s3(
        self,
        source_key=None,
        target_key=None,
        file_type='csv',
        chunk_size=100000,
        dtype=None,
        skip_rows=None,
        compression=None,
        keep_original_name_locally=False,
        encoding='utf-8',
        pre_process_chunk=None,
        kwargs={}
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
        :param compression: string, the compression type for the chunk
        :param keep_original_name_locally: boolean, if True it will extract the file name form the key
                                                    if False it will generate a uuid for the file name
        :param encoding: string, the encoding of the chunk
        :param pre_process_chunk: function, the function that holds the preprocess method for the chunk
        :param kwargs: dict, the arguments for the preprocess function
        """

        output_file_name = None

        # From the key get all available objects in a list
        list_of_objects = self.aws_client.list_s3_files_from_folder(
            bucket=self.source_bucket,
            key=source_key
        )
        logger.info('Found {} objects in the path s3://{}/{}'.
                    format(len(list_of_objects), self.source_bucket, source_key))
        logger.info('Initiating splitting...')

        # For each object download it and convert it to parquet
        for object in list_of_objects:

            # Get the file name from the object
            if keep_original_name_locally:
                output_file_name = object.split('/')[-1].split('.')[0] + '.' \
                                   + '.'.join(object.split('/')[-1].split('.')[1:])

            # Download the file from S3
            file_name = self.aws_client.download_from_s3(key=object,
                                                         bucket=self.source_bucket,
                                                         output_file_name=output_file_name)

            self.convert_from_local(
                file_name=file_name,
                target_key=target_key,
                file_type=file_type,
                chunk_size=chunk_size,
                dtype=dtype,
                skip_rows=skip_rows,
                compression=compression,
                encoding=encoding,
                pre_process_chunk=pre_process_chunk,
                kwargs=kwargs
            )

        logger.info('Done.')

    def convert_from_local(
        self,
        file_name=None,
        target_key=None,
        file_type='csv',
        chunk_size=100000,
        dtype=None,
        skip_rows=None,
        compression=None,
        encoding='utf-8',
        pre_process_chunk=None,
        kwargs={}
    ):
        """
        Convert files to Parquet locally and upload them to a specific key to S3
        :param file_name: string, the name of the file locally
        :param target_key: string, the key where the file will be uploaded to, it can be a folder from S3 also
        :param file_type: string, the type of data to process
        :param chunk_size: integer the number of rows per chunk
        :param dtype: dict, the definitions of the csv columns
        :param skip_rows: integer, the number of columns to skip on the CSV
        :param compression: string, the compression type for the chunk
        :param encoding: string, the encoding of the chunk
        :param pre_process_chunk: function, the function that holds the preprocess method for the chunk
        :param kwargs: dict, the arguments for the preprocess function
        """

        for part in self.convert(
                file_type=file_type,
                file_name=file_name,
                dtype=dtype,
                chunk_size=chunk_size,
                compression=compression,
                skip_rows=skip_rows,
                encoding=encoding,
                pre_process_chunk=pre_process_chunk,
                kwargs=kwargs
        ):

            # if the key does not ends in `/` then concat a `/` after the key
            target_path = target_key + part
            if target_key[-1] != '/':
                target_path = target_key + '/' + part

            self.aws_client.upload_to_s3(bucket=self.target_bucket, key=target_path, file_name=part)

            # Upload part to S3
            logger.info('Part {} uploaded to s3://{}/{}'.format(part, self.target_bucket, target_key))

            os.unlink(os.path.join(os.getcwd(), part))

        os.unlink(os.path.join(os.getcwd(), file_name))

        logger.info('Done splitting file {}'.format(file_name))

    def convert(
        self,
        file_name=None,
        file_type='csv',
        chunk_size=100000,
        dtype=None,
        skip_rows=None,
        compression=None,
        encoding='utf-8',
        pre_process_chunk=None,
        kwargs={}
    ):
        """
        Convert the file to parquet locally
        :param file_name: string, the name of the file locally
        :param file_type: string, the type of data to process
        :param chunk_size: integer the number of rows per chunk
        :param dtype: dict, the definitions of the csv columns
        :param skip_rows: integer, the number of columns to skip on the CSV
        :param compression: string, the compression type for the chunk
        :param encoding: string, the encoding of the chunk
        :param pre_process_chunk: function, the function that holds the preprocess method for the chunk
        :param kwargs: dict, the arguments for the preprocess function
        :return: yields the file name of the part
        """

        utils = Utils()

        logger.info('Splitting file {}...'.format(file_name))

        for part in utils._parquetify(
                file_type=file_type,
                file_name=file_name,
                dtype=dtype,
                chunksize=chunk_size,
                compression=compression,
                skip_rows=skip_rows,
                encoding=encoding,
                pre_process_chunk=pre_process_chunk,
                kwargs=kwargs
        ):

            yield part
