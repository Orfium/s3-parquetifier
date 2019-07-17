import json
import traceback
import uuid
import os

import boto3
import botocore
from botocore.exceptions import ClientError


class AWSClient:

    def __init__(self, region='us-west-2', access_key=None, secret_key=None):
        """
        Make sure the credentials are initialized and exist
        :param region: str, the region of your ecosystem in AWS, default value is us-west-2 (Oregon)
        :param access_key: str, the access key from your AWS Account
        :param secret_key: str, the secret key from your AWS Account
        """
        # Get the AWS credentials
        self.region = region
        if access_key and secret_key:
            aws_access_key = access_key
            aws_secret_key = secret_key
        else:
            # Get the AWS credentials
            aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID', None)
            aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', None)

        self.region = region
        if not aws_access_key or not aws_secret_key:
            self.session = boto3.session.Session()
        else:
            self.session = boto3.session.Session(
                region_name=self.region,
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key
            )

        self.access_key = access_key
        self.secret_key = secret_key

    def download_from_s3(self, key=None, bucket=None, output_file_name=None):
        """
        Download a file from S3 given the filename and the bucket
        :param key: the file name in S3
        :param bucket: the bucket the filename exists to
        :param output_file_name: the name of the downloaded file
        :return: the path of the downloaded file
        """
        if not output_file_name:
            output_file_name = str(uuid.uuid4())

        # Sanity check fo the imported data
        if not key or not bucket:
            raise ValueError('Please provide a key and a bucket to download from s3')

        # Connect to S3
        s3 = boto3.resource('s3', region_name=self.region,
                            aws_access_key_id=self.access_key,
                            aws_secret_access_key=self.secret_key)

        # Try to download the file from S3
        try:
            s3.Bucket(bucket).download_file(key, output_file_name)
        except ClientError as e:
            if e.response['Error']['Code'] == "404":  # File not found
                ClientError("The object does not exist.")
            else:  # Catch general code errors and report
                raise Exception("Unexpected error: %s" % e)

            return False

        return output_file_name

    @staticmethod
    def read_file_from_s3(key=None, bucket=None):
        """
        Read a file from S3 line by line without downloading it
        :param key: str, the name of the file in S3
        :param bucket: str, the name of the bucket where the file exists
        :return: yields the lines of the file
        """
        raise NotImplementedError('Please use `read_s3_file_lines` for now.')

    def upload_to_s3_obj(self, file_name=None, bucket=None, key=None):
        """
        Upload a file to s3 from local
        :param file_name: the filename of the file in the local machine
        :param bucket: the bucket where you want to upload the file
        :param key: the name of the file in s3 after the upload
        :return: the response from the upload procedure, False if error
        """
        # Sanity check to imported data
        if not file_name or not bucket or not key:
            raise ValueError('Please provide the path for the file, the bucket and the new filename in bucket.')

        # Connect to S3
        s3 = boto3.client('s3', region_name=self.region,
                          aws_access_key_id=self.access_key,
                          aws_secret_access_key=self.secret_key)

        with open(file_name, 'rb') as data:
            return s3.upload_fileobj(data, bucket, key)

    def upload_to_s3(self, file_name=None, bucket=None, key=None):
        """
        Upload a file to s3 from local
        :param file_name: the filename of the file in the local machine
        :param bucket: the bucket where you want to upload the file
        :param key: the name of the file in s3 after the upload
        :return: the response from the upload procedure, False if error
        """
        # Sanity check to imported data
        if not file_name or not bucket or not key:
            raise ValueError('Please provide the path for the file, the bucket and the new filename in bucket.')

        # Connect to S3
        s3 = boto3.client('s3', region_name=self.region,
                          aws_access_key_id=self.access_key,
                          aws_secret_access_key=self.secret_key)

        return s3.upload_file(file_name, bucket, key)

    def list_s3_files_from_folder(self, bucket=None, key=None):
        """
        List all folders and files in an S3 folder and or a bucket
        :param bucket: the bucket the file is in
        :param key: the name of the file
        :param delimiter: string, the delimiter which splits the object key into folders
        :return: list, list of items
        """

        if not bucket:
            raise ValueError('Please provide a bucket')

        s3 = boto3.client('s3', region_name=self.region,
                          aws_access_key_id=self.access_key,
                          aws_secret_access_key=self.secret_key)

        objects = s3.list_objects(
            Bucket=bucket,
            # Delimiter='/',
            Prefix=key
        )

        list_of_objects = []
        if 'Contents' in objects:
            for object in objects['Contents']:
                if key + '/' != object['Key']:
                    # path = key + '/' + object['Key'] if key else object['Key']
                    list_of_objects.append(object['Key'])

            return list_of_objects

        return []

    def check_if_object_exists(self, key=None, bucket=None):
        """
        Checks if an object exists in an S3 bucket
        :param bucket: the bucket the file is in
        :param key: the name of the file
        :return: boolean
        """

        if not bucket or not key:
            raise ValueError('Please provide a bucket and a key')

        s3 = boto3.resource('s3', region_name=self.region,
                            aws_access_key_id=self.access_key,
                            aws_secret_access_key=self.secret_key)

        try:
            s3.Object(bucket, key).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                # Something else has gone wrong.
                return False
        else:
            # The object does exist.
            return True

    def check_if_bucket_exists(self, bucket=None):
        """
        Checks if a bucket exists or the client has access
        :param bucket: string, the bucket name
        :return: boolean, True if exists, False if not
        """

        if not bucket:
            raise ValueError('Please provide a bucket.')

        s3 = boto3.resource('s3', region_name=self.region,
                            aws_access_key_id=self.access_key,
                            aws_secret_access_key=self.secret_key)

        try:
            s3.meta.client.head_bucket(Bucket=bucket)

            return True
        except ClientError as e:
            print('Error checking the bucket')
            print(str(e))
            traceback.print_exc()

            return False
