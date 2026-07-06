from datetime import datetime

from airflow.decorators import dag
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)


@dag(
    dag_id="saas_product_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["saas", "data-engineering", "aws"],
)
def saas_product_pipeline():

    generate_saas_events = LambdaInvokeFunctionOperator(
        task_id="generate_saas_events",
        function_name="saas-event-generator",
    )

    run_glue_etl = GlueJobOperator(
        task_id="run_glue_etl",
        job_name="saas-events-etl",
    )

    run_glue_crawler = GlueCrawlerOperator(
        task_id="run_glue_crawler",
        config={
            "Name": "saas-events-crawler",
        },
    )

    # Set task dependencies
    generate_saas_events >> run_glue_etl >> run_glue_crawler


dag = saas_product_pipeline()