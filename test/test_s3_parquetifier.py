from shutil import copyfile
from unittest import TestCase
import os
import pandas as pd
import logging
from unittest import TestCase
from s3_parquetifier import S3Parquetifier
from unittest.mock import patch


@patch("botocore.client.BaseClient._make_api_call")
class TestS3Parquetifier(TestCase):
    csv1 = os.path.join(os.path.dirname(__file__), "data/test_csv1.csv")
    csv2 = os.path.join(os.path.dirname(__file__), "data/test_csv2.csv")
    csv_skiprows = os.path.join(os.path.dirname(__file__), "data/test_csv_for_skip_rows.csv")
    csv_gziped = os.path.join(os.path.dirname(__file__), "data/test_csv1.csv.gz")

    def setUp(self):
        logging.getLogger("siakon.s3Parquetifier").propagate = False

    def get_files(self, number=1):
        file1 = os.path.join(os.path.dirname(__file__), "data/archive/test_csv1.csv")
        copyfile(file1, self.csv1)

        if number > 1:
            file2 = os.path.join(os.path.dirname(__file__), "data/archive/test_csv2.csv")
            copyfile(file2, self.csv2)

    def get_skip_rows_file(self):
        file1 = os.path.join(os.path.dirname(__file__), "data/archive/test_csv_for_skip_rows.csv")
        copyfile(file1, self.csv_skiprows)

    def get_gziped_file(self):
        file1 = os.path.join(os.path.dirname(__file__), "data/archive/test_csv1.csv.gz")
        copyfile(file1, self.csv_gziped)

    def test_split_csv_valid(self, boto_mock):
        self.get_files()

        total = 0
        for file_name in S3Parquetifier("source_bucket", "target_bucket")._parquetify(
            file_name=self.csv1, chunksize=10
        ):
            chunk = pd.read_parquet(file_name)
            total += sum(chunk["Sunspots"])

            os.unlink(file_name)

        os.unlink(self.csv1)

        self.assertEqual(total, 2187)

    def test_split_csv_with_skip_rows(self, boto_mock):
        self.get_skip_rows_file()

        total = 0
        for file_name in S3Parquetifier("source_bucket", "target_bucket")._parquetify(
            file_name=self.csv_skiprows, chunksize=10, skip_rows=1
        ):
            chunk = pd.read_parquet(file_name)
            total += sum(chunk["Sunspots"])

            os.unlink(file_name)

        os.unlink(self.csv_skiprows)

        self.assertEqual(total, 2187)

    def test_split_csv_with_compression(self, boto_mock):
        self.get_gziped_file()

        total = 0
        for file_name in S3Parquetifier("source_bucket", "target_bucket")._parquetify(
            file_name=self.csv_gziped, chunksize=10
        ):
            chunk = pd.read_parquet(file_name)
            total += sum(chunk["Sunspots"])

            os.unlink(file_name)

        os.unlink(self.csv_gziped)

        self.assertEqual(total, 2187)

    def test_split_csv_with_dtype(self, boto_mock):
        self.get_files()

        total = 0
        for file_name in S3Parquetifier("source_bucket", "target_bucket")._parquetify(
            file_name=self.csv1, chunksize=10, dtype={"Month": "utf8", "Sunsports": "float"}
        ):
            chunk = pd.read_parquet(file_name)
            total += sum(chunk["Sunspots"])

            os.unlink(file_name)

        os.unlink(self.csv1)

        self.assertEqual(total, 2187)

    def test_parquetify_csv_valid(self, boto_mock):
        self.get_files()

        total = 0
        for file_name in S3Parquetifier("source_bucket", "target_bucket")._parquetify(
            file_name=self.csv1, chunksize=10, dtype={"Month": "utf8", "Sunsports": "float"}
        ):
            file_path = os.path.join(os.getcwd(), file_name)
            df_temp = pd.read_parquet(file_path)

            total += sum(df_temp["Sunspots"])

            os.unlink(file_path)

        self.assertEqual(total, 2187)

    def test_parquetify_csv_valid_with_compression(self, boto_mock):
        self.get_files()

        total = 0
        for file_name in S3Parquetifier("source_bucket", "target_bucket")._parquetify(
            file_name=self.csv1, chunksize=10, dtype={"Month": "utf8", "Sunsports": "float"},
        ):

            file_path = os.path.join(os.getcwd(), file_name)
            df_temp = pd.read_parquet(file_path)

            total += sum(df_temp["Sunspots"])

            os.unlink(file_path)

        self.assertEqual(total, 2187)

    def test_parquetify_csv_with_preprocessor(self, boto_mock):
        self.get_files()

        def pre_process(chunk):
            chunk["Sunspots"] = chunk["Sunspots"] + 1
            dtype = {"Month": "utf8", "Sunsports": "float"}

            return dtype, chunk

        total = 0
        for file_name in S3Parquetifier("source_bucket", "target_bucket")._parquetify(
            file_name=self.csv1, chunksize=10, pre_process_chunk=pre_process,
        ):

            file_path = os.path.join(os.getcwd(), file_name)
            df_temp = pd.read_parquet(file_path)

            total += sum(df_temp["Sunspots"])

            os.unlink(file_path)

        self.assertEqual(total, 2187 + 28)

    def test_parquetify_csv_with_extra_columns(self, boto_mock):
        self.get_files()

        def pre_process(chunk):
            chunk["Extra Column"] = None
            dtype = {"Month": "utf8", "Sunsports": "float", "Extra Column": "utf8"}

            return dtype, chunk

        for file_name in S3Parquetifier("source_bucket", "target_bucket")._parquetify(
            file_name=self.csv1, chunksize=10, pre_process_chunk=pre_process,
        ):

            file_path = os.path.join(os.getcwd(), file_name)
            df_temp = pd.read_parquet(file_path)

            self.assertIn("Extra Column", list(df_temp.columns))

            os.unlink(file_path)

    def test_parquetify_csv_with_extra_columns_custom_value(self, boto_mock):
        self.get_files()

        def pre_process(chunk, value=None):
            chunk["Extra Column"] = value
            dtype = {"Month": "utf8", "Sunsports": "float", "Extra Column": "utf8"}

            return dtype, chunk

        for file_name in S3Parquetifier("source_bucket", "target_bucket")._parquetify(
            file_name=self.csv1,
            chunksize=10,
            pre_process_chunk=pre_process,
            kwargs={"value": "test"},
        ):

            file_path = os.path.join(os.getcwd(), file_name)
            df_temp = pd.read_parquet(file_path)

            self.assertIn("Extra Column", list(df_temp.columns))
            self.assertEqual(df_temp.iloc[0]["Extra Column"], "test")

            os.unlink(file_path)
