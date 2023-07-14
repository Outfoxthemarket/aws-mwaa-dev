from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from dependencies import DAG_81_code as code

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
DEFAULT_ARGS = {
    "depends_on_past": False,
    "email": ["alli.issa@outfoxthemarket.co.uk", "marcos.martinez@outfoxthemarket.co.uk"],
    "email_on_failure": False,
    "email_on_retry": False,
    "email_on_success": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
        dag_id="Elexon_S0142",
        # These args will get passed on to each operator
        # You can override them on a per-task basis during operator initialization
        default_args=DEFAULT_ARGS,
        description="Settlement Period Data",
        start_date=datetime(2023, 7, 12),
        schedule="0 5 * * 1-5",  # At 05:00 on every day-of-week from Monday through Friday.
        catchup=False,
        tags=["Alli", "G", "Elexon", "Beta"],
) as dag:
    fetch = PythonOperator(
        task_id="Fetch_Elexon_zip",
        python_callable=code.fetch_elexon_data
    )

    slaveII = PythonOperator(
        task_id="Unzip_Elexon_SlaveII",
        python_callable=code.slave_unzip,
        op_kwargs={"file": "II"}
    )

    slaveSF = PythonOperator(
        task_id="Unzip_Elexon_SlaveSF",
        python_callable=code.slave_unzip,
        op_kwargs={"file": "SF"}
    )

    slaveR1 = PythonOperator(
        task_id="Unzip_Elexon_SlaveR1",
        python_callable=code.slave_unzip,
        op_kwargs={"file": "R1"}
    )

    slaveR2 = PythonOperator(
        task_id="Unzip_Elexon_SlaveR2",
        python_callable=code.slave_unzip,
        op_kwargs={"file": "R2"}
    )

    slaveR3 = PythonOperator(
        task_id="Unzip_Elexon_SlaveR3",
        python_callable=code.slave_unzip,
        op_kwargs={"file": "R3"}
    )

    slaveRF = PythonOperator(
        task_id="Unzip_Elexon_SlaveRF",
        python_callable=code.slave_unzip,
        op_kwargs={"file": "RF"}
    )

    slaveDF = PythonOperator(
        task_id="Unzip_Elexon_SlaveDF",
        python_callable=code.slave_unzip,
        op_kwargs={"file": "DF"}
    )

    ReduceII = PythonOperator(
        task_id="Reduce_II",
        python_callable=code.reduce_data,
        op_kwargs={"file": "II"}
    )

    ReduceSF = PythonOperator(
        task_id="Reduce_SF",
        python_callable=code.reduce_data,
        op_kwargs={"file": "SF"}
    )

    ReduceR1 = PythonOperator(
        task_id="Reduce_R1",
        python_callable=code.reduce_data,
        op_kwargs={"file": "R1"}
    )

    ReduceR2 = PythonOperator(
        task_id="Reduce_R2",
        python_callable=code.reduce_data,
        op_kwargs={"file": "R2"}
    )

    ReduceR3 = PythonOperator(
        task_id="Reduce_R3",
        python_callable=code.reduce_data,
        op_kwargs={"file": "R3"}
    )

    ReduceRF = PythonOperator(
        task_id="Reduce_RF",
        python_callable=code.reduce_data,
        op_kwargs={"file": "RF"}
    )

    ReduceDF = PythonOperator(
        task_id="Reduce_DF",
        python_callable=code.reduce_data,
        op_kwargs={"file": "DF"}
    )

    fetch >> [slaveII, slaveSF, slaveR1, slaveR2, slaveR3, slaveRF, slaveDF]
    slaveII >> ReduceII
    slaveSF >> ReduceSF
    slaveR1 >> ReduceR1
    slaveR2 >> ReduceR2
    slaveR3 >> ReduceR3
    slaveRF >> ReduceRF
    slaveDF >> ReduceDF
