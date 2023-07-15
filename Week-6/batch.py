#!/usr/bin/env python
# coding: utf-8
import os
import pickle
import pandas as pd


def get_input_path(year, month):
    default_input_pattern = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    return input_pattern.format(year=year, month=month)


def get_output_path(year, month):
    default_output_pattern = 's3://nyc-duration/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
    return output_pattern.format(year=year, month=month)


def main(year: int, month: int):
    endpoint_url = os.getenv('S3_ENDPOINT_URL')
    if endpoint_url:
        options = {
            'client_kwargs': {
                'endpoint_url': endpoint_url
            },
            "key": "test",
            "secret": "test",
        }

    input_file = get_input_path(year, month)
    output_file = get_output_path(year, month)
    # output_file = f'output/yellow_tripdata_{year:04d}-{month:02d}.parquet'

    categorical = ['PULocationID', 'DOLocationID']

    df = read_data(input_file, categorical)
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    dicts = df[categorical].to_dict(orient='records')

    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)

    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)

    print('predicted mean duration:', y_pred.mean())

    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred

    df_result.to_parquet(output_file, engine='pyarrow', index=False, storage_options=options)


def read_data(filename: str, categorical: list):
    # endpoint_url = os.getenv('S3_ENDPOINT_URL', None)
    # if endpoint_url:
    #     options = {
    #         'client_kwargs': {
    #             'endpoint_url': endpoint_url
    #         }
    #     }
    #     df = pd.read_parquet(filename, storage_options=options)
    # else:
    df = pd.read_parquet(filename)
    return prepare_data(df, categorical)


def prepare_data(df: pd.DataFrame, categorical: list) -> pd.DataFrame:
    df_copy = df.copy()

    df_copy['duration'] = df_copy.tpep_dropoff_datetime - df_copy.tpep_pickup_datetime
    df_copy['duration'] = df_copy.duration.dt.total_seconds() / 60

    df_copy = df_copy[(df_copy.duration >= 1) & (df_copy.duration <= 60)].copy()

    df_copy[categorical] = df_copy[categorical].fillna(-1).astype('int').astype('str')
    return df_copy


if __name__ == '__main__':
    main(2022, 2)
