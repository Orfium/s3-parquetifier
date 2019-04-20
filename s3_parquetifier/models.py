import logging
from .utils import logger
from .aws_client import AWSClient


class S3Parquetifier:

    def __init__(
            self,
            aws_access_key=None,
            aws_secret_key=None,
            region='us-west-2',
            verbose=False,
            source_bucket=None,
            target_bucket=None
    ):
        """
        Make sure you have the proper credentials to access the AWS buckets and Glue
        :param aws_access_key: the AWS access key from your AWS Account
        :param aws_secret_key: the AWS secret key from your AWS Account
        :param verbose: boolean, set this to true to see logs
        :param source_bucket: string, the bucket where the files are
        :param target_bucket: string, the bucket for the parquet files
        """

        if not aws_access_key or not aws_secret_key:
            raise ValueError('No credentials for AWS are provided.')

        if not source_bucket or not target_bucket:
            raise ValueError('Please provide a source and a target bucket.')

        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.region = region

        # Check if the buckets exists or the client has access
        self.aws_client = AWSClient(secret_key=aws_secret_key, access_key=aws_access_key, region=region)

        if not self.aws_client.check_if_bucket_exists(bucket=source_bucket):
            raise ValueError('Bucket {} not found or Access denied.'.format(source_bucket))

        if not self.aws_client.check_if_bucket_exists(bucket=target_bucket):
            raise ValueError('Bucket {} not found or Access denied.'.format(target_bucket))

        self.source_bucket = source_bucket
        self.targer_bucket = target_bucket

        logging.getLogger('siakon.s3Parquetifier').propagate = verbose

    def convert(
            self,
            source_key=None,
            target_key=None,
            file_type='csv',
            chunk_size=100000,
            dtype=None,
            skip_rows=None,
            extra_columns=None,
            compression=None
    ):
        """
        Convert a file or a series or files from a bucket to another
        :param source_key:
        :param target_key:
        :param file_type:
        :param chunk_size:
        :param dtype:
        :param skip_rows:
        :param extra_columns:
        :param compression:
        :return:
        """

        #
        pass

    def download_file(self, url_target=None):
        pass

    def list_s3(self):
        pass
