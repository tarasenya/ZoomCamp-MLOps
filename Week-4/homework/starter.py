import os
import pickle
import pandas as pd
import numpy
import click

CATEGORICAL = ['PULocationID', 'DOLocationID']


def get_pickled_models(path_to_pickle: str):
    with open(path_to_pickle, 'rb') as f_in:
        dv, model = pickle.load(f_in)
    return dv, model


def read_data(filename, categorical=CATEGORICAL):
    df = pd.read_parquet(filename)

    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')

    return df


def get_predictions(path_to_pickle, df: pd.DataFrame):
    dv, model = get_pickled_models(path_to_pickle)

    dicts = df[CATEGORICAL].to_dict(orient='records')

    X_val = dv.transform(dicts)

    y_pred = model.predict(X_val)
    print(f'Standatad deviation equals {numpy.std(y_pred)}')
    return y_pred


def enrich_with_id(df: pd.DataFrame, year: int, month: int, y_pred: list):
    df_result = df.copy()
    df_result['ride_id'] = f'{year:04d}/{month:02d}_' + df_result.index.astype('str')
    df_result['predictions'] = y_pred
    return df_result


def save_to_parquet(data, output_file: str, path_to_output_dir: str = 'output') -> None:
    data.to_parquet(
        os.path.join(path_to_output_dir, output_file),
        engine='pyarrow',
        compression=None,
        index=False)
    return None


@click.command()
@click.option(
    "--path_to_pickle",
    help="Location of pickle",
    type=str
)
@click.option(
    "--year",
    help="Year for evaluation",
    type=int
)
@click.option(
    "--month",
    help="Month for evaluation",
    type=int
)
def create_df_with_prediction_and_save_to_parquet(path_to_pickle: str, year: int, month: int):
    df = read_data(f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet')
    y_pred = get_predictions(path_to_pickle=path_to_pickle, df=df)
    print(f'Mean of predictions {numpy.mean(y_pred)}')
    df_result = enrich_with_id(df, 2022, 2, y_pred)
    df_results = df_result[['ride_id', 'predictions']]
    save_to_parquet(df_results, f'yellow_tripdata_{year:04d}-{month:02d}_with_predictions.parquet')


if __name__ == "__main__":
    create_df_with_prediction_and_save_to_parquet()
