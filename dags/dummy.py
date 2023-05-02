from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from dependencies import dummy_code as code

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
DEFAULT_ARGS = {
    "depends_on_past": False,
    "email": ["marcos.martinez@outfoxthemarket.co.uk"],
    "email_on_failure": False,
    "email_on_retry": False,
    "email_on_success": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
        dag_id="Dummy",
        # These args will get passed on to each operator
        # You can override them on a per-task basis during operator initialization
        default_args=DEFAULT_ARGS,
        description="Quarterly generation of FIR report",
        start_date=datetime(2023, 1, 1),
        schedule="30 9 15 4,7,10,1 *",  # At 09:30 on day-of-month 15 in April, July, October, and January.
        catchup=False,
        tags=["Marcos", "Dummy", "Test"],
) as dag:
    task1 = PythonOperator(
        task_id="First",
        python_callable=code.dummy
    )

    task2 = PythonOperator(
        task_id="Second",
        python_callable=code.dummy2
    )

    task1 >> task2
