from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from dependencies import DAG_71_code as code

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
DEFAULT_ARGS = {
    "depends_on_past": False,
    "email": ["louise.holford@outfoxthemarket.co.uk", "marcos.martinez@outfoxthemarket.co.uk"],
    "email_on_failure": False,
    "email_on_retry": False,
    "email_on_success": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
        dag_id="DD_Adequacy",
        # These args will get passed on to each operator
        # You can override them on a per-task basis during operator initialization
        default_args=DEFAULT_ARGS,
        description="DD Adequacy",
        start_date=datetime(2023, 5, 25),
        schedule="0 13 * */3 1",  # At 09:00 on Monday in every 3rd month
        catchup=False,
        tags=["Louise", "DD", "Adequacy", "Beta"],
) as dag:

    task1 = PythonOperator(
        task_id="Fetch_Daily_Report_RED_FISH",
        python_callable=code.fetch_daily_data
    )

    task2 = PythonOperator(
        task_id="Fetch_DD_Report_RED_FISH",
        python_callable=code.fetch_dd_report
    )

    task3 = PythonOperator(
        task_id="Unzip_Daily_Report_RED_FISH",
        python_callable=code.unzip_daily_report
    )

    task4 = PythonOperator(
        task_id="Unzip_DD_Report_RED_FISH",
        python_callable=code.unzip_dd_report
    )

    task5 = PythonOperator(
        task_id="Anniversary_Date_Daily_Report",
        python_callable=code.anniversary_date_daily_report
    )

    task1 >> task3 >> task5
    task2 >> task4