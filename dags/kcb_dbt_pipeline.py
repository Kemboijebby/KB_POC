from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "kcb_data_platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="kcb_dbt_streaming_transformation",
    default_args=default_args,
    description="Orchestrates ClickHouse dbt staging and mart models for loan & risk KPIs",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["kcb", "clickhouse", "dbt", "loans"],
) as dag:

    # 1. Compile dbt models
    dbt_compile = BashOperator(
        task_id="dbt_compile",
        bash_command="cd /opt/airflow/kcb_dbt && python -m dbt.cli.main compile --profiles-dir /home/airflow/.dbt",
    )

    # 2. Run Staging layer
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="cd /opt/airflow/kcb_dbt && python -m dbt.cli.main run --select staging --profiles-dir /home/airflow/.dbt",
    )

    # 3. Run Marts layer
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /opt/airflow/kcb_dbt && python -m dbt.cli.main run --select marts --profiles-dir /home/airflow/.dbt",
    )

    # 4. Execute data quality tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/kcb_dbt && python -m dbt.cli.main test --profiles-dir /home/airflow/.dbt",
    )

    dbt_compile >> dbt_run_staging >> dbt_run_marts >> dbt_test
    