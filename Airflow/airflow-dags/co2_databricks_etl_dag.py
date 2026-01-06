"""
Airflow DAG to orchestrate CO₂ Emissions ETL Pipeline
Bronze → Silver → Gold using Databricks Jobs
"""

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta
import logging

# -----------------------
# Default DAG Arguments
# -----------------------
default_args = {
    "owner": "Bhavani",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# -----------------------
# DAG Definition
# -----------------------
with DAG(
    dag_id="co2_emissions_databricks_etl",
    description="Airflow DAG to trigger Databricks ETL pipeline (Bronze → Silver → Gold)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",   # can be None for manual runs
    catchup=False,
    tags=["co2", "etl", "databricks"],
) as dag:

    logging.info("Starting CO₂ Emissions ETL DAG")

    # -----------------------
    # Trigger Databricks Job
    # -----------------------
    run_co2_etl_job = DatabricksRunNowOperator(
        task_id="run_co2_databricks_job",
        databricks_conn_id="databricks_default",
        job_id=786321176354304  
    )

    run_co2_etl_job
