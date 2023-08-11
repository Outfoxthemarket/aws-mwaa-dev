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
    print(f"[+] Fetching Elexon data for {TARGET_DAY} from ftp://ftp.elexonportal.co.uk{ROOT_DIR}")
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
        print(f"[+] File Downloaded from S3 -- {os.path.isfile(f'/usr/local/airflow/{file}-{i}.gz')}")
        with gzip.open(f"/usr/local/airflow/{file}-{i}.gz") as z:
            S3.put_object(Key=f"tmp/DAG-81/{file}/{file}-{i}.txt", Body=z.read(), ContentType='text/txt')
        os.remove(f"/usr/local/airflow/{file}-{i}.gz")
        print(f'[+] File removed from local -- {not os.path.isfile(f"/usr/local/airflow/{file}-{i}.gz")}')
        S3.Object(f"tmp/DAG-81/{file}/{file}-{i}.gz").delete()
        print(f"[+] File deleted from S3")


def reduce_data(ti, file):
    print(f"[+] Number of files to reduce: {ti.xcom_pull(key=file)}")
    with io.StringIO() as report:
        print(f"[+] Report object: {report}")
        for i in range(1, ti.xcom_pull(key=file) + 1):
            print(f"[+] Downloading {file}-{i}.txt from S3")
            with io.StringIO(
                    S3.Object(f"tmp/DAG-81/{file}/{file}-{i}.txt").get()['Body'].read().decode('utf-8')) as partition:
                print(f"[+] Reducing {file}-{i}.txt")
                for line in partition:
                    contents = line.split("|")
                    if contents[0] == "SRH":
                        date = datetime.strptime(contents[1], "%Y%m%d")
                    elif contents[0] == "SPI":
                        settlementPeriod = contents[1]
                        assert int(settlementPeriod) <= 50, "Settlement Period is greater than 50"
                    elif contents[0] == "BPI" and any(
                            ["OFTM" in contents[1], "FXGL" in contents[1], "SPAL" in contents[1]]):
                        output = [date.strftime("%d/%m/%Y"),
                                  settlementPeriod,
                                  contents[2].split("__")[-1],
                                  contents[1].split("__")[1][1:5],  # 4-digit code for the supplier
                                  contents[-4],
                                  f'{contents[-3]}\n']
                        report.write(";".join(output))
                    else:
                        continue
        for i in range(1, ti.xcom_pull(key=file) + 1):
            S3.Object(f"tmp/DAG-81/{file}/{file}-{i}.txt").delete()
            print(f"[+] Finished reducing {file}-{i}.txt")
        S3.put_object(Key=f"tmp/DAG-81/reduce/{file}.csv", Body=report.getvalue(), ContentType='text/csv')
        report.flush()
