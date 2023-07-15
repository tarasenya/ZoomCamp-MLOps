#!/usr/bin/env python
# coding: utf-8
import os
import pickle

import numpy as np
import pandas as pd


def get_input_path(year, month):
    default_input_pattern = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    return input_pattern.format(year=year, month=month)


def get_output_path(year, month):
    default_output_pattern = 's3://nyc-duration/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
    return output_pattern.format(year=year, month=month)


def main(year: int, month: int) -> float:
    input_file = get_input_path(year, month)
    output_file = get_output_path(year, month)

    categorical = ['PULocationID', 'DOLocationID']

    df = read_data(input_file, categorical)
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    dicts = df[categorical].to_dict(orient='records')

    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)

    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)

    print('predicted mean duration:', y_pred.mean())
    print(f'the sum of predicted durations {np.sum(y_pred)}')
    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred
    save_data(output_file, df_result)
    return round(np.sum(y_pred), 2)


def read_data(filename: str, categorical: list):
    endpoint_url = os.getenv('S3_ENDPOINT_URL')
    options = {
        'client_kwargs': {
            'endpoint_url': endpoint_url
        },
        "key": "test",
        "secret": "test",
    }
    df = pd.read_parquet(filename, storage_options=options)

    return prepare_data(df, categorical)


def save_data(file_name: str, df: pd.DataFrame) -> None:
    endpoint_url = os.getenv('S3_ENDPOINT_URL')
    options = {
        'client_kwargs': {
            'endpoint_url': endpoint_url
        },
        "key": "test",
        "secret": "test",
    }
    df.to_parquet(file_name, engine='pyarrow', index=False, storage_options=options)
    return None


def prepare_data(df: pd.DataFrame, categorical: list) -> pd.DataFrame:
    df_copy = df.copy()

    df_copy['duration'] = df_copy.tpep_dropoff_datetime - df_copy.tpep_pickup_datetime
    df_copy['duration'] = df_copy.duration.dt.total_seconds() / 60

    df_copy = df_copy[(df_copy.duration >= 1) & (df_copy.duration <= 60)].copy()

    df_copy[categorical] = df_copy[categorical].fillna(-1).astype('int').astype('str')
    return df_copy


if __name__ == '__main__':
    main(2022, 2)
