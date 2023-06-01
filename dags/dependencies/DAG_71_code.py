import io
import os
from datetime import datetime
import boto3
import pandas as pd
from airflow.providers.ftp.hooks.ftp import FTPSHook
from airflow.models import Variable
import zipfile

S3 = boto3.resource('s3').Bucket('zogsolutions-eu-west-2-mwaa')
RED_FISH = FTPSHook(ftp_conn_id="RED_FISH")
RED_FISH.conn_type = "ftps"


def fetch_daily_data():
    with io.BytesIO() as buffer:
        with RED_FISH.get_conn() as server:
            server.login(user="FEReports", passwd=Variable.get("RED_FISH_FTPS_PASSWORD"))
            server.prot_p()  # needed for FTPS (ssl)
            server.cwd(f"Daily Reports/{datetime.now().strftime('%Y%m%d')}")
            server.retrbinary(f"RETR DailyLiveReport.zip", buffer.write)
            print(f"[+] Downloading DailyLiveReport.zip from {server.pwd()}")
        S3.put_object(Key="tmp/DAG-71/DailyLiveReport.zip", Body=buffer.getvalue(), ContentType='application/zip')
    print("[+] Downloaded DailyLiveReport.zip - saved into tmp/")


def fetch_dd_report():
    with io.BytesIO() as buffer:
        with RED_FISH.get_conn() as server:
            server.login(user="FEReports", passwd=Variable.get("RED_FISH_FTPS_PASSWORD"))
            server.prot_p()  # needed for FTPS (ssl)
            server.cwd(f"Daily Reports/{datetime.now().strftime('%Y%m%d')}")
            server.retrbinary(f"RETR DDAdequacy.zip", buffer.write)
            print(f"[+] Downloading DDAdequacy.zip from {server.pwd()}")
        S3.put_object(Key="tmp/DAG-71/DDAdequacy.zip", Body=buffer.getvalue(), ContentType='application/zip')
    print("[+] Downloaded DDAdequacy.zip - saved into tmp/")


def unzip_daily_report():
    print("[+] Downloading DailyLiveReport.zip from S3")
    S3.Object('tmp/DAG-71/DailyLiveReport.zip').download_file('/usr/local/airflow/DailyLiveReport.zip')
    print(os.listdir("/usr/local/airflow"))
    with zipfile.ZipFile('/usr/local/airflow/DailyLiveReport.zip') as z:
        z.extractall("/usr/local/airflow", pwd=Variable.get("RED_FISH_ZIP_PASSWORD").encode())
    print("[+] Unzipped DailyLiveReport.zip - saved locally")
    df = pd.read_excel("/usr/local/airflow/DailyLiveReport.xlsx", usecols=["ACCOUNT_NO", "CURRENT_TARIFF_START"])
    df["CURRENT_TARIFF_START"] = pd.to_datetime(df["CURRENT_TARIFF_START"], format="%d/%m/%Y")
    print(df.head(1))
    S3.put_object(Key="tmp/DAG-71/DailyLiveReport.csv", Body=df.to_csv(index=False), ContentType='text/csv')
    print('[+] Removing local files (consistency))')
    os.remove("/usr/local/airflow/DailyLiveReport.xlsx")
    os.remove("/usr/local/airflow/DailyLiveReport.zip")
    print(os.listdir("/usr/local/airflow"))


def unzip_dd_report():
    print("[+] Downloading DDAdequacy.zip from S3")
    S3.Object('tmp/DAG-71/DDAdequacy.zip').download_file('/usr/local/airflow/DDAdequacy.zip')
    print(os.listdir("/usr/local/airflow"))
    with zipfile.ZipFile('/usr/local/airflow/DDAdequacy.zip') as z:
        z.extractall("/usr/local/airflow", pwd=Variable.get("RED_FISH_ZIP_PASSWORD").encode())
    print("[+] Unzipped DDAdequacy.zip - saved locally")
    df = pd.read_excel("/usr/local/airflow/DDAdequacy.xlsx", usecols=['ACCOUNT_NO','account_balance','direct_debit_amount','billing_day','annual_cost','region_id','has_elec','has_gas','total_eac','total_aq'])
    df.columns = df.columns.str.upper()
    print(df.head(1))
    S3.put_object(Key="tmp/DAG-71/DDAdequacy.csv", Body=df.to_csv(index=False), ContentType='text/csv')
    print('[+] Removing local files (consistency))')
    os.remove("/usr/local/airflow/DDAdequacy.xlsx")
    os.remove("/usr/local/airflow/DDAdequacy.zip")
    print(os.listdir("/usr/local/airflow"))


def anniversary_date_daily_report():
    df = pd.read_csv(S3.Object("tmp/DAG-71/DailyLiveReport.csv").get()['Body'])
    df['CURRENT_TARIFF_START'] = pd.to_datetime(df['CURRENT_TARIFF_START'])

    def get_anniversary(date):
        this_year = datetime(datetime.now().year, date.month, date.day)
        if this_year > datetime.now():
            return this_year
        else:
            return datetime(datetime.now().year + 1, date.month, date.day)

    df['ANNIVERSARY_DATE'] = df['CURRENT_TARIFF_START'].apply(get_anniversary)
    df['MONTHS_TILL_ANNIVERSARY'] = (df['ANNIVERSARY_DATE'].dt.month - datetime.now().month) % 12
    df['MONTHS_TILL_ANNIVERSARY'] = df['MONTHS_TILL_ANNIVERSARY'].apply(lambda date: date + 12 if date <= 3 else date)
    S3.put_object(Key="tmp/DAG-71/DailyLiveReport.csv", Body=df.to_csv(index=False), ContentType='text/csv')

