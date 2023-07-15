#!/bin/bash
export INPUT_FILE_PATTERN="s3://nyc-duration/in/{year:04d}-{month:02d}.parquet"
export OUTPUT_FILE_PATTERN="s3://nyc-duration/out/{year:04d}-{month:02d}.parquet"
export S3_ENDPOINT_URL="http://localhost:4566"

pipenv run python integration_test.py

ERROR_CODE=$?
if [ ${ERROR_CODE} !=0 ]
then
  exit ${ERROR_CODE}
fi

echo "Success"
