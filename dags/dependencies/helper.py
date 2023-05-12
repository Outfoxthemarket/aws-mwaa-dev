import boto3
import pandas as pd

S3 = boto3.resource('s3').Bucket('zogsolutions-eu-west-2-mwaa')


def write_df_s3_csv(df: pd.DataFrame, key: str):
    S3.put_object(Key=key, Body=df.to_csv(), ContentType='text/csv')


def read_df_s3_csv(key: str) -> pd.DataFrame:
    return pd.read_csv(S3.Object(key).get()['Body'])
