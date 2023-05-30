from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from dependencies import DAG_73_code as code

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
DEFAULT_ARGS = {
    "depends_on_past": False,
    "email": ["alli.issa@outfoxthemarket.co.uk", "marcos.martinez@outfoxthemarket.co.uk"],
    "email_on_failure": True,
    "email_on_retry": False,
    "email_on_success": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
        dag_id="Alli_Automation",
        # These args will get passed on to each operator
        # You can override them on a per-task basis during operator initialization
        default_args=DEFAULT_ARGS,
        description="Report with fuel and system data form BMRS",
        start_date=datetime(2023, 5, 25),
        schedule="30 8 * * 1-5",  # At 8 30 every day.
        catchup=False,
        tags=["Alli", "Elexon", "BMRS", "Beta"],
) as dag:

    task1 = PythonOperator(
        task_id="Fetch_System_Data",
        python_callable=code.fetch_system_data
    )

    task2 = PythonOperator(
        task_id="Fetch_Generation_Data",
        python_callable=code.fetch_generation_data
    )

    task3 = PythonOperator(
        task_id="Send_Email",
        python_callable=code.send_email
    )

    [task1, task2] >> task3