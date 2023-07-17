from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.smtp.hooks.smtp import SmtpHook


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
DEFAULT_ARGS = {
    "depends_on_past": False,
    "email": ["marcos.martinez@outfoxthemarket.co.uk"],
    "email_on_failure": True,
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
        description="Send email to get my deposit back",
        start_date=datetime(2023, 1, 1),
        schedule="0 9 * * *",
        catchup=False,
        tags=["Marcos", "Dummy", "Test"],
) as dag:

    def send_email():
        with SmtpHook(smtp_conn_id="SMTP_SENDGRID") as mail:
            mail.send_email_smtp(
                to="Marcos.Martinez@outfoxthemarket.co.uk",  #"info@brinkriley.co.uk",
                from_email="almighty@email.com",
                subject="Dummy",
                html_content="Dummy",
            )

    task1 = PythonOperator(
        task_id="Send_Email",
        python_callable=send_email
    )

    task1
