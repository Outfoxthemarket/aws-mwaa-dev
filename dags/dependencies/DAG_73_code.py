import io
import pandas as pd
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import boto3

S3 = boto3.resource('s3').Bucket('zogsolutions-eu-west-2-mwaa')
BASE_URL = 'https://api.bmreports.com/BMRS'
API_KEY = Variable.get('ALLI_ELEXON_API_KEY')


def fetch_system_data():
    yesterday = datetime.now() - timedelta(days=1)
    print("[+] Used date - ", yesterday.strftime('%Y-%m-%d'))
    endpoint = 'B1770'
    response = requests.get(BASE_URL + f'/{endpoint}/V1', params={
        'APIKey': API_KEY,
        'SettlementDate': yesterday.strftime('%Y-%m-%d'),
        'Period': "*",
        'ServiceType': 'csv'
    })
    print("[+] Endpoint - ", response.url)
    df = pd.read_csv(response.url, usecols=['SettlementDate', 'SettlementPeriod', 'ImbalancePriceAmount'], skiprows=4)
    df.iloc[:, 2] = pd.to_datetime(df.iloc[:, 2], format='%Y-%m-%d')
    print(df.head())
    S3.put_object(Key="tmp/DAG-73/system.csv", Body=df.to_csv(index=False, date_format='%d/%m/%Y'), ContentType='text/csv')


def fetch_generation_data():
    yesterday = datetime.now() - timedelta(days=1)
    print("[+] Used date - ", yesterday.strftime('%Y-%m-%d'))
    endpoint = 'FUELHH'
    response = requests.get(BASE_URL + f'/{endpoint}/V1', params={
        'APIKey': API_KEY,
        'FromDate': yesterday.strftime('%Y-%m-%d'),
        'ToDate': yesterday.strftime('%Y-%m-%d'),
        'ServiceType': 'csv'
    })
    print("[+] Endpoint - ", response.url)
    df = pd.read_csv(response.url, skiprows=1)
    df.drop(df.tail(1).index, inplace=True)
    df.iloc[:, 1] = pd.to_datetime(df.iloc[:, 1], format='%Y%m%d')
    print(df.head())
    S3.put_object(Key="tmp/DAG-73/generation.csv", Body=df.to_csv(index=False, date_format='%d/%m/%Y'), ContentType='text/csv')


def send_email():
    with io.BytesIO() as buffer:
        with pd.ExcelWriter(buffer) as writer:
            generation = pd.read_csv(S3.Object("tmp/DAG-73/generation.csv").get()['Body']).sort_values(by=['SettlementDate', 'SettlementPeriod'])
            generation.to_excel(writer, sheet_name='System', index=False)
            system = pd.read_csv(S3.Object("tmp/DAG-73/system.csv").get()['Body'], header=None)
            system.columns = [
                'HDR',
                'Settlement Date',
                'Settlement Period',
                'CCGT',
                'Oil',
                'Coal',
                'Nuclear',
                'Wind',
                'Pumped Storage',
                'Hydro',
                'OCGT',
                'Other',
                'INTFR',
                'INTIRL(Northern IrelandMoyle)',
                'INTED(NetherlandsBritNed)',
                'INTEW(Ireland East West)',
                'Biomass',
                'INTNEM(Belgium Nemo Link)',
                'INTEL(France Eleclink)',
                'INTIFA2(France IFA2)',
                'INTNSL(Norway2 North Sea link)'
            ]
            system.to_excel(writer, sheet_name='Generation', index=False)
            S3.put_object(Key=f"artifacts/DAG-73/{datetime.now().strftime('%Y-%m-%d')}/report.xlsx", Body=buffer.getvalue(), ContentType='text/xlsx')
    print('[+] Sending email')
