from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.utils.dates import days_ago

PROJECT_ID = "fast-level-up-487010"
REGION = "us-central1"
# This path points to the Classic Template file you created
TEMPLATE_PATH = "gs://dataflow_practice_101/templates/pubsub_to_gcs_bq"

default_args = {
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="trigger_dataflow_pubsub_to_bq",
    default_args=default_args,
    description="Triggers Dataflow streaming job from CLASSIC template",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    start_dataflow = DataflowTemplatedJobStartOperator(
        task_id="start_dataflow_streaming_job",
        template=TEMPLATE_PATH,
        project_id=PROJECT_ID,
        location=REGION,
        # For Classic Templates, parameters and environment are passed directly
        parameters={
            "input_topic": "projects/fast-level-up-487010/topics/orders-topic",
            "output_table": "fast-level-up-487010:test.orders_stream",
        },
        environment={
            "tempLocation": "gs://dataflow_practice_101/temp",
            "zone": "us-central1-a",
            "machineType": "e2-medium",
        },
    )


"""
gcloud composer environments run my-airflow   --location us-central1   dags backfill -- simple_python_dag   --start-date 2026-02-09   --end-date 2026-02-10
"""
