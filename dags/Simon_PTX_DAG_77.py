from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from dependencies import DAG_77_code as code

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
DEFAULT_ARGS = {
    "depends_on_past": False,
    "email": ["simon.dowse@foxglove.energy", "marcos.martinez@outfoxthemarket.co.uk"],
    "email_on_failure": False,
    "email_on_retry": False,
    "email_on_success": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
        dag_id="Download_PTX_Reports",
        # These args will get passed on to each operator
        # You can override them on a per-task basis during operator initialization
        default_args=DEFAULT_ARGS,
        description="Download PTX Reports",
        start_date=datetime(2023, 5, 25),
        schedule="0 7 * * 1-5",
        catchup=False,
        tags=["Simon", "PTX", "Beta"],
) as dag:

    task1 = PythonOperator(
        task_id="Get_Session_ID_Handshake",
        python_callable=code.get_session_id,
    )

    task1