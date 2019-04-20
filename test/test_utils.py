from shutil import copyfile
from unittest import TestCase
import os
from s3_parquetifier.utils import Utils
import pandas as pd
import logging


class TestUtils(TestCase):
    csv1 = os.path.join(os.path.dirname(__file__), 'data/test_csv1.csv')
    csv2 = os.path.join(os.path.dirname(__file__), 'data/test_csv2.csv')
    csv_skiprows = os.path.join(os.path.dirname(__file__), 'data/test_csv_for_skip_rows.csv')
    csv_gziped = os.path.join(os.path.dirname(__file__), 'data/test_csv1.csv.gz')

    def setUp(self):
        logging.getLogger('siakon.s3Parquetifier').propagate = False

    def get_files(self, number=1):
        file1 = os.path.join(os.path.dirname(__file__), 'data/archived/test_csv1.csv')
        copyfile(file1, self.csv1)

        if number > 1:
            file2 = os.path.join(os.path.dirname(__file__), 'data/archived/test_csv2.csv')
            copyfile(file2, self.csv2)

    def get_skip_rows_file(self):
        file1 = os.path.join(os.path.dirname(__file__), 'data/archived/test_csv_for_skip_rows.csv')
        copyfile(file1, self.csv_skiprows)

    def get_gziped_file(self):
        file1 = os.path.join(os.path.dirname(__file__), 'data/archived/test_csv1.csv.gz')
        copyfile(file1, self.csv_gziped)

    def test_split_csv_valid(self):
        self.get_files()

        total = 0
        for chunk in Utils().get_chunk_from_file(file_name=self.csv1, chunksize=10):
            total += sum(chunk['Sunspots'])

        os.unlink(self.csv1)

        self.assertEqual(total, 2187)

    def test_split_csv_with_skip_rows(self):
        self.get_skip_rows_file()

        total = 0
        for chunk in Utils().get_chunk_from_file(file_name=self.csv_skiprows, chunksize=10, skip_rows=1):
            total += sum(chunk['Sunspots'])

        os.unlink(self.csv_skiprows)

        self.assertEqual(total, 2187)

    def test_split_csv_with_compression(self):
        self.get_gziped_file()

        total = 0
        for chunk in Utils().get_chunk_from_file(file_name=self.csv_gziped, chunksize=10):
            total += sum(chunk['Sunspots'])

        os.unlink(self.csv_gziped)

        self.assertEqual(total, 2187)

    def test_split_csv_with_dtype(self):
        self.get_files()

        total = 0
        for chunk in Utils().get_chunk_from_file(file_name=self.csv1, chunksize=10,
                                                 dtype={"Month": str, "Sunsports": float}):
            total += sum(chunk['Sunspots'])

        os.unlink(self.csv1)

        self.assertEqual(total, 2187)

    def test_parquetify_csv_valid(self):
        self.get_files()

        utils = Utils()

        total = 0
        for file_name in utils._parquetify(file_name=self.csv1,
                                           chunksize=10,
                                           dtype={"Month": str, "Sunsports": float}):
            file_path = os.path.join(os.getcwd(), file_name)
            df_temp = pd.read_parquet(file_path)

            total += sum(df_temp['Sunspots'])

            os.unlink(file_path)

        self.assertEqual(total, 2187)

    def test_parquetify_csv_valid_with_compression(self):
        self.get_files()

        utils = Utils()

        total = 0
        for file_name in utils._parquetify(file_name=self.csv1,
                                           chunksize=10,
                                           dtype={"Month": str, "Sunsports": float},
                                           compression='gzip'):

            file_path = os.path.join(os.getcwd(), file_name)
            df_temp = pd.read_parquet(file_path)

            total += sum(df_temp['Sunspots'])

            os.unlink(file_path)

        self.assertEqual(total, 2187)

    def test_parquetify_csv_with_preprocessor(self):
        self.get_files()

        utils = Utils()

        def pre_process(chunk):
            chunk['Sunspots'] = chunk['Sunspots'] + 1

            return chunk

        total = 0
        for file_name in utils._parquetify(file_name=self.csv1,
                                           chunksize=10,
                                           dtype={"Month": str, "Sunsports": float},
                                           pre_process_chunk=pre_process):

            file_path = os.path.join(os.getcwd(), file_name)
            df_temp = pd.read_parquet(file_path)

            total += sum(df_temp['Sunspots'])

            os.unlink(file_path)

        self.assertEqual(total, 2187 + 28)

    def test_parquetify_csv_with_extra_columns(self):
        self.get_files()

        utils = Utils()

        def pre_process(chunk):
            chunk['Extra Column'] = None

            return chunk

        for file_name in utils._parquetify(file_name=self.csv1,
                                           chunksize=10,
                                           dtype={"Month": str, "Sunsports": float},
                                           pre_process_chunk=pre_process):

            file_path = os.path.join(os.getcwd(), file_name)
            df_temp = pd.read_parquet(file_path)

            self.assertIn('Extra Column', list(df_temp.columns))

            os.unlink(file_path)

    def test_parquetify_csv_with_extra_columns_custom_value(self):
        self.get_files()

        utils = Utils()

        def pre_process(chunk, value=None):
            chunk['Extra Column'] = value

            return chunk

        for file_name in utils._parquetify(file_name=self.csv1,
                                           chunksize=10,
                                           dtype={"Month": str, "Sunsports": float},
                                           pre_process_chunk=pre_process,
                                           kwargs={"value": "test"}):

            file_path = os.path.join(os.getcwd(), file_name)
            df_temp = pd.read_parquet(file_path)

            self.assertIn('Extra Column', list(df_temp.columns))
            self.assertEqual(df_temp.iloc[0]['Extra Column'], 'test')

            os.unlink(file_path)
