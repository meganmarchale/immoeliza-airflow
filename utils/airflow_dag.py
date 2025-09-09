from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
import os, subprocess, sys, json

# Store config in Airflow Variables or env
REPO_DIR = Variable.get("IMMOVLAN_REPO_DIR", "/opt/airflow/dags/repo")
PYTHON_BIN = Variable.get("PYTHON_BIN", sys.executable)
SCRAPER_SCRIPT = os.path.join(REPO_DIR, "scraper.py")

default_args = {
    "owner": "data",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

@dag(
    dag_id="immovlan_daily",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",   # or "0 2 * * *"
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
)
def immovlan_daily():
    @task
    def scrape():
        # run your scraper as a subprocess (or import the module and call main())
        cmd = [PYTHON_BIN, SCRAPER_SCRIPT, "--mode", "daily"]
        print("Running:", " ".join(cmd))
        subprocess.check_call(cmd)

    @task
    def clean_transform():
        # e.g., call a python module that cleans CSVs into parquet
        return "ok"

    @task
    def upsert_to_db():
        # write CSV -> Postgres with upsert (see SQL below)
        return "ok"

    @task
    def train_model():
        # load features, train, log with MLflow, persist model artifact
        return "ok"

    @task
    def refresh_app_cache():
        # optional: ping Streamlit endpoint, or write a cache file in S3/GCS
        return "ok"

    s = scrape()
    c = clean_transform()
    u = upsert_to_db()
    t = train_model()
    r = refresh_app_cache()

    s >> c >> u >> t >> r

immovlan_daily()
