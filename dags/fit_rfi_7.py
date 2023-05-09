from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import dependencies.fit_7.fit_code as fit

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
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}


with DAG(
    dag_id="FIT_RFI_v0.5",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args=DEFAULT_ARGS,
    description="Quarterly generation of FIR report",
    start_date=datetime(2023, 1, 1),
    schedule="30 9 15 4,7,10,1 *", # At 09:30 on day-of-month 15 in April, July, October, and January.
    catchup=False,
    tags=["FIT", "RFI", "Quarterly", "Marcos"],
) as dag:

    task1 = PythonOperator(
        task_id="Download_CVS_Quicksight",
        python_callable=fit.download_raw_data
    )

    task2 = PythonOperator(
        task_id="Merge_Check_Sum",
        python_callable=fit.merge_check_sum
    )

    task3 = PythonOperator(
        task_id="Forecast_Missing_SF",
        python_callable=fit.forecast_missing_sf
    )

    task4 = PythonOperator(
        task_id="Generate_FIT_Report",
        python_callable=fit.generate_fit_results
    )

    task1 >> task2 >> task3 >> task4
        