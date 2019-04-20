# S3 Parquetifier
[![Build Status](https://semaphoreci.com/api/v1/projects/e5f4d811-2000-4e01-a0e5-eb695ebc92d6/2651638/shields_badge.svg)](https://semaphoreci.com/thelastdev/s3-parquetifier)
[![PyPI version fury.io](https://badge.fury.io/py/s3-parquetifier.svg)](https://pypi.org/project/s3-parquetifier/)
[![MIT license](https://img.shields.io/badge/License-MIT-blue.svg)](https://lbesson.mit-license.org/)

S3 Parquetifier is an ETL tool that can take a file from an S3 bucket convert it to Parquet format and
save it to another bucket.

S3 Parquetifier supports the following file types
- [x] CSV
- [ ] JSON
- [ ] TSV

## Instructions

### How to install

To install the package just run the following

```sh
sudo apt-get install -y libssl-dev libffi-dev &&
sudo apt-get install -y libxml2-dev libxslt1-dev &&
sudo apt-get install -y libsnappy-dev
```

```python
pip install s3-parquetifier
```

### How to use it

S3 parquetifier needs an AWS Account that will have at least read rights for the target bucket
and read-write rights for the destination bucket. 

You can read the following article on how to set up S3 roles and policies [here](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_examples_s3_rw-bucket.html)

#### Running the Script

```python
from s3_parquetifier import S3Parquetifier

# Call the covertor
S3Parquetifier(
    source_bucket='<the bucket's name where the CSVs are>',
    target_bucket='<the bucket's name where you want the parquet file to be saved>',
    verbose=True,  # for verbosity or not
).convert_from_s3(
    source_key='<the key of the S3 object>',
    target_key='<the key of the S3 object>',
    chunk_size=100000  # The number of rows per parquet
)
```

```python
from s3_parquetifier import S3Parquetifier

# Call the covertor
S3Parquetifier(
    target_bucket='<the bucket\'s name where you want the parquet file to be saved>',
    verbose=True,  # for verbosity or not
).convert_from_local(
    file_name='<The CSV file that you want to transform>',
    target_key='<The S3 bucket key where the file will be saved>',
    chunk_size=100000,
)
```

### Adding custom pre-processing function

You can add custom pre-processing function on your source file. Because this tool is designed for large files the preprocessing
is taking place on every chunk separately. If the full file is needed for the preprocessing then a local preprocessing is needed in the source file.

In the following example, we are going to add custom columns on the chunk with some custom values.
We are going to add the columns `test1, test2, test3` with the values `1, 2, 3` respectively.

We define our function bellow named `pre_process` and we also define the arguments for the function `kwargs`.
The chunk DataFrame is not needed in the kwargs, it is taken by default. You have to pass your function as an argument in
`pre_process_chunk` and the arguments in `kwargs` in the `convert_from_s3` method.

```python
from s3_parquetifier import S3Parquetifier


# Add three new columns with custom values
def pre_process(chunk, columns=None, values=None):

    for index, column in enumerate(columns):
        chunk[column] = values[index]

    return chunk

# define the arguments for the pre-processor
kwargs = {
    'columns': ['test1', 'test2', 'test3'],
    'values': [1, 2, 3]
}

# Call the covertor
S3Parquetifier(
    source_bucket='<the bucket's name where the CSVs are>',
    target_bucket='<the bucket's name where you want the parquet file to be saved>',
    verbose=True,  # for verbosity or not
).convert_from_s3(
    source_key='<the key of the S3 object>',
    target_key='<the key of the S3 object>',
    chunk_size=100000  # The number of rows per parquet
    pre_process_chunk=pre_process,  # A preprocessing function that will pre-process the each chunk
    kwargs=kwargs  # potential extra arguments for the pre-preocess function
)
```

## ToDo

- [x] Add support to handle local files too
- [ ] Add support for JSON
- [ ] Add streaming from url support

