from io import StringIO

import pandas as pd
import os
from .logger import logger
import requests


class Utils:

    def _parquetify(
            self,
            file_type='csv',
            file_name=None,
            chunksize=1000000,
            skip_rows=None,
            dtype=None,
            compression=None,
            encoding='utf-8',
            pre_process_chunk=None,
            kwargs={}
    ):
        """
        Given a file split it into segments and upload it to S3 in a parquet format.
        :param file_name: string, the name of the file locally or a url of the file
        :param skip_rows: integer, how many rows should skip from the csv
        :param dtype: dict, the type of each column in the csv
        :param compression: string, the allowed compression from pandas lib
        """

        file_number = 1

        for chunk in self.get_chunk_from_file(file_type=file_type,
                                              file_name=file_name,
                                              skip_rows=skip_rows,
                                              chunksize=chunksize,
                                              dtype=dtype,
                                              encoding=encoding):

            # apply any pre-processing in the chunk
            if pre_process_chunk:
                chunk = pre_process_chunk(chunk=chunk, **kwargs)

            # construct the name of the part
            chunk_name = file_name.split('.'+file_type)[0].split('/')[-1] + '_part_%s.parquet%s' % \
                         (str(file_number).zfill(4), '.' + compression if compression else '')

            # export the part as parquet and compressed if needed
            chunk.to_parquet(chunk_name, compression=compression)
            # logger.info('File %s is generated.' % chunk_name)

            # yield the generated part for upload
            yield chunk_name
            file_number += 1

        return

    @staticmethod
    def get_chunk_from_file(
            file_type='csv',
            file_name=None,
            skip_rows=None,
            chunksize=None,
            dtype=None,
            encoding='utf-8',
    ):

        # Open the CSV file with pandas
        if file_type == 'csv':
            for chunk in pd.read_csv(file_name,
                                     skiprows=skip_rows,
                                     chunksize=chunksize,
                                     dtype=dtype,
                                     encoding=encoding,
                                     low_memory=False if dtype else True):
                yield chunk
        else:
            raise NotImplementedError()

    @staticmethod
    def stream_csv_from_url(
            file_name,
            skip_rows,
            chunksize
    ):

        logger.warning('THIS IS NOT A COMPLETE FEATURE. IT MAY CONTAIN BUGS. USE WITH CAUTION!')
        # Stream the file from the url
        r = requests.get(file_name, stream=True)

        # The size of the chunk
        # buffer_size = 1024 * 1024 * 5  # 64MB per run
        buffer_size = 1024 * 2

        # This is used to get the remainder of the chunk
        remainder = ""

        # The number of lines processed
        processed = 0

        batch = ""
        headers = None
        skipped_rows = 0
        for chunk in r.iter_content(chunk_size=buffer_size):
            data = (remainder + chunk.decode('utf-8')).replace('\r\n', '\n')

            for line in data.splitlines()[:-1]:

                # Bypass `skip_rows` lines before getting the header
                if not headers:
                    if skip_rows:
                        if skipped_rows < skip_rows:
                            skipped_rows += 1
                            continue

                    headers = line
                    continue

                # If the chunksize is met construct a pandas DataFrame and yield it
                if processed >= chunksize:
                    batch = headers + '\n' + batch

                    f = StringIO(batch)

                    yield pd.read_csv(f, delimiter=',', quotechar='"')

                    processed = 0
                    batch = ""

                batch += line + '\n'
                processed += 1

                if not chunk:
                    break

            remainder = data.splitlines()[-1]

            if data[-1] in ['\n', '\r']:
                remainder += '\n'

        # for the remainder of the file that did not hit the chunksize
        batch = headers + '\n' + batch + '\n' + remainder

        f = StringIO(batch)

        yield pd.read_csv(f, delimiter=',', quotechar='"')
