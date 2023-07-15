import os
from datetime import datetime
import pandas as pd


def dt(hour, minute, second=0):
    return datetime(2022, 1, 1, hour, minute, second)


def get_input_path(year, month) -> str:
    output_pattern = "s3://nyc-duration/in/{year:04d}-{month:02d}.parquet"
    return output_pattern.format(year=year, month=month)


def create_test_data_and_put_to_s3():
    endpoint_url = os.getenv('S3_ENDPOINT_URL')

    if endpoint_url:
        options = {
            'client_kwargs': {
                'endpoint_url': endpoint_url
            },
            "key": "test",
            "secret": "test",
        }

    data = [
        (None, None, dt(1, 2), dt(1, 10)),
        (1, None, dt(1, 2), dt(1, 10)),
        (1, 2, dt(2, 2), dt(2, 3)),
        (None, 1, dt(1, 2, 0), dt(1, 2, 50)),
        (2, 3, dt(1, 2, 0), dt(1, 2, 59)),
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)),
    ]

    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
    df = pd.DataFrame(data, columns=columns)
    output_file = get_input_path(2022, 1)

    df.to_parquet(output_file, engine='pyarrow', compression=None, index=False, storage_options=options)


if __name__ == '__main__':
    create_test_data_and_put_to_s3()
