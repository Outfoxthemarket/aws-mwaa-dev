import io
import os
from datetime import datetime
import boto3
import pandas as pd
from airflow.providers.ftp.hooks.ftp import FTPSHook
from airflow.models import Variable
import zipfile
import numpy as np

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
    df = pd.read_excel("/usr/local/airflow/DDAdequacy.xlsx",
                       usecols=['ACCOUNT_NO', 'account_balance', 'direct_debit_amount', 'billing_day', 'annual_cost',
                                'region_id', 'has_elec', 'has_gas', 'total_eac', 'total_aq'])
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


def cost_calculations_dd_report():
    dd = pd.read_csv(S3.Object("tmp/DAG-71/DDAdequacy.csv").get()['Body'])
    dd['ABS_ACCOUNT_BALANCE'] = np.abs(dd['ACCOUNT_BALANCE'])
    dd['ANNUAL_COST_INC_VAT'] = dd['ANNUAL_COST'] * 1.05
    dd['ANNUAL_COST_INC_VAT'] = dd['ANNUAL_COST_INC_VAT'].apply(lambda x: round(x, 2))
    dd['MONTHLY_COST_INC_VAT'] = dd['ANNUAL_COST_INC_VAT'] / 12
    dd['MONTHLY_COST_INC_VAT'] = dd['MONTHLY_COST_INC_VAT'].apply(lambda x: round(x, 2))
    dd['CREDIT_OR_DEBIT'] = dd['ACCOUNT_BALANCE'].apply(lambda x: 'CREDIT' if x < 0 else 'DEBIT')
    S3.put_object(Key="tmp/DAG-71/DDAdequacy.csv", Body=dd.to_csv(index=False), ContentType='text/csv')


def pricing():
    print("[+] Dummy prices")
    pass


def calculate_adequate_dd_report():
    days = {
        'Q1': 90,
        'Q2': 91,
        'Q3': 92,
        'Q4': 92
    }

    def get_end_quarter():
        match 'Q' + str((datetime.now().month - 1) // 3 + 1):
            case 'Q1':
                return datetime(datetime.now().year, 3, 31)
            case 'Q2':
                return datetime(datetime.now().year, 6, 30)
            case 'Q3':
                return datetime(datetime.now().year, 9, 30)
            case 'Q4':
                return datetime(datetime.now().year, 12, 31)

    percentage = (get_end_quarter() - datetime.now()).days / days['Q' + str((datetime.now().month - 1) // 3 + 1)]
    print(f'[+] Percentage of quarter passed: {percentage}')
    print("[+] Downloading Dummy-prices.csv from S3")
    dummy_prices = pd.read_csv(S3.Object("tmp/DAG-71/Dummy-prices.csv").get()['Body'])
    dd = pd.read_csv(S3.Object("tmp/DAG-71/DDAdequacy.csv").get()['Body'])
    dd = dd.merge(dummy_prices, on='REGION_ID', how='left')
    dd['YEARLY_ELEC_COST'] = dd.apply(lambda row: (row['ELEC_STANDING_CHARGE'] * 365 + row['ELEC_UNIT_RATE'] * row['TOTAL_EAC']) / 100 if row['HAS_ELEC'] else 0, axis=1)
    dd['YEARLY_ELEC_COST'] = dd['YEARLY_ELEC_COST'].apply(lambda x: round(x, 2))
    dd['YEARLY_GAS_COST'] = dd.apply(lambda row: (row['GAS_STANDING_CHARGE'] * 365 + row['GAS_UNIT_RATE'] * row['TOTAL_AQ']) / 100 if row['HAS_GAS'] else 0, axis=1)
    dd['YEARLY_GAS_COST'] = dd['YEARLY_GAS_COST'].apply(lambda x: round(x, 2))
    dd['YEARLY_TOTAL_COST'] = dd['YEARLY_ELEC_COST'] + dd['YEARLY_GAS_COST']
    dd['NEW_ADEQUATE_DD'] = dd['YEARLY_TOTAL_COST'] / 12
    S3.put_object(Key="tmp/DAG-71/DDAdequacy.csv", Body=dd.to_csv(index=False), ContentType='text/csv')


def new_dd_adequate():
    pass
