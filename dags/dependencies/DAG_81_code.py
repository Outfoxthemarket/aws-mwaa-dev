import boto3
from datetime import datetime, timedelta
import gzip
import io
import os
from airflow.providers.ftp.hooks.ftp import FTPHook

S3 = boto3.resource('s3').Bucket('zogsolutions-eu-west-2-mwaa')
ELEXON = FTPHook(ftp_conn_id="ELEXON-ALLI")
TARGET_DAY = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
ROOT_DIR = f"/p114/{TARGET_DAY}"


def fetch_elexon_data(ti):
    print(f"[+] Fetching Elexon data for {TARGET_DAY} from {ROOT_DIR}")
    count = {
        "DF": 0,
        "II": 0,
        "SF": 0,
        "R1": 0,
        "R2": 0,
        "R3": 0,
        "RF": 0,
    }
    with ELEXON.get_conn() as server:
        with io.BytesIO() as buffer:
            server.cwd(ROOT_DIR)
            for file in [file for file in server.nlst(ROOT_DIR) if file.startswith("S0142")]:
                print(f"[+] Downloading {file} from {ROOT_DIR}")
                file_type = file.split("_")[2]
                count[file_type] += 1
                server.retrbinary(f"RETR {file}", buffer.write)
                S3.put_object(Key=f"tmp/DAG-81/{file_type}/{file_type}-{count[file_type]}.gz", Body=buffer.getvalue(),
                              ContentType='application/gzip')
                print(f"[+] Uploaded {file} to S3")
                buffer.flush()
    for key, value in count.items():
        ti.xcom_push(key=key, value=value)


def slave_unzip(ti, file):
    print(f"[+] Number of files to unzip: {ti.xcom_pull(key=file)}")
    for i in range(1, ti.xcom_pull(key=file) + 1):
        print(f"[+] Downloading {file}-{i}.gz from S3")
        S3.Object(f"tmp/DAG-81/{file}/{file}-{i}.gz").download_file(f"/usr/local/airflow/{file}-{i}.gz")
        print(f"[+] File Downloaded from S3 -- {os.path.isfile(f'/usr/local/airflow/{file}-{i}.zip')}")
        with gzip.open(f"/usr/local/airflow/{file}-{i}.gz") as z:
            S3.put_object(Key=f"tmp/DAG-81/{file}/{file}-{i}.csv", Body=z.read(), ContentType='text/csv')
        os.remove(f"/usr/local/airflow/{file}-{i}.gz")
        print(f'[+] File removed from local -- {not os.path.isfile(f"/usr/local/airflow/{file}-{i}.gz")}')
