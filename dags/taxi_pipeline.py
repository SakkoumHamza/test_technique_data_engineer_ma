import logging
from datetime import datetime
from uuid import uuid4
from airflow.decorators import dag, task


@dag(
    dag_id="taxi_pipeline",
    description="Download and publish a Chicago taxi snapshot",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["taxi", "local"],
)
def taxi_pipeline():
    @task
    def ingest() -> dict:
        from pipeline.download import download_to_bronze

      
        snapshot_id = uuid4().hex
        path, count = download_to_bronze(snapshot_id)
        logging.info("Bronze snapshot %s contains %s rows", path, count)
        return {"path": path, "count": count, "snapshot_id": snapshot_id}

    @task
    def transform_and_publish(bronze: dict) -> dict:
        from pipeline.processing import process

        return process(bronze["path"], bronze["snapshot_id"], bronze["count"], logging.getLogger(__name__))

    transform_and_publish(ingest())


taxi_pipeline()
