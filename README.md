# S3 Parquetifier
[![Build Status](https://semaphoreci.com/api/v1/projects/e5f4d811-2000-4e01-a0e5-eb695ebc92d6/2651638/shields_badge.svg)](https://semaphoreci.com/thelastdev/s3-parquetifier)

S3 Parquetifier is an ETL tool that can take a file from an S3 bucket convert it to Parquet format and
save it to another bucket.

S3 Parquetifier supports the following files
- [x] CSV
- [ ] JSON
- [ ] TSV

## Instructions

### How to install

To install the package just run the following

```python
pip install git+git://github.com/siakon89/s3-parquetifier.git
```

### How to use it

S3 parquetifier needs an AWS Account that will have at least read rights for the target bucket
and read-write rights for the destination bucket. 

You can read the following article on how to set up S3 roles and policies [here](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_examples_s3_rw-bucket.html)

#### Running the Script

### Adding custom preprocessing

## ToDo

- [ ] Add support for JSON
- [ ] Add streaming from url support
- [ ] Add support to handle local files too