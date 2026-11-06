import io
import os
import logging
from datetime import datetime

import boto3
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# --- Конфигурация ---
S3_ENDPOINT = "https://storage.yandexcloud.net"
BUCKET = "final-project"
BATCHES = [f"transactions_batch_{i}.csv" for i in range(1, 11)]

# Для локального PostgreSQL (Docker-контейнер практикума)
PG_DSN = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": os.getenv("PG_PORT", "5432"),
    "dbname": os.getenv("PG_DB", "de"),
    "user": os.getenv("PG_USER", "jovyan"),
    "password": os.getenv("PG_PASSWORD", "jovyan"),
}

TX_COLS = (
    "operation_id, account_number_from, account_number_to, currency_code, country, "
    "status, transaction_type, amount, transaction_dt, transaction_date"
)

def get_s3_client():
    """Инициализация S3 клиента через Airflow Variables."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=Variable.get("s3_access_key", default_var=""),
        aws_secret_access_key=Variable.get("s3_secret_key", default_var=""),
    )

def load_day(ds, **kwargs):
    """
    Еженедельный/ежедневный ETL: читает батчи из S3, фильтрует по дате ds 
    и идемпотентно загружает в staging-слой PostgreSQL.
    """
    s3 = get_s3_client()
    
    # 1. Сбор транзакций за день из всех батчей
    frames = []
    for key in BATCHES:
        log.info("Чтение батча %s", key)
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        df = pd.read_csv(io.BytesIO(obj["Body"].read()))
        
        df = df.dropna(subset=["transaction_dt"])
        df["transaction_dt"] = pd.to_datetime(df["transaction_dt"])
        df["transaction_date"] = df["transaction_dt"].dt.strftime("%Y-%m-%d")
        
        day_df = df[df["transaction_date"] == ds]
        if not day_df.empty:
            frames.append(day_df)

    if frames:
        tx = pd.concat(frames, ignore_index=True).drop_duplicates()
    else:
        tx = pd.DataFrame(columns=[
            "operation_id", "account_number_from", "account_number_to",
            "currency_code", "country", "status", "transaction_type",
            "amount", "transaction_dt", "transaction_date"
        ])

    # 2. Курсы валют за день
    cur_obj = s3.get_object(Bucket=BUCKET, Key="currencies_history.csv")
    cur_df = pd.read_csv(io.BytesIO(cur_obj["Body"].read()))
    cur_df["date_update"] = pd.to_datetime(cur_df["date_update"]).dt.strftime("%Y-%m-%d")
    cur_day = cur_df[cur_df["date_update"] == ds]

    # 3. Идемпотентная запись в staging (PostgreSQL)
    conn = psycopg2.connect(**PG_DSN)
    try:
        with conn.cursor() as c:
            c.execute("DELETE FROM staging.transactions WHERE transaction_date = %s", (ds,))
            if not tx.empty:
                execute_values(
                    c, 
                    f"INSERT INTO staging.transactions ({TX_COLS}) VALUES %s",
                    [
                        (r.operation_id, int(r.account_number_from), int(r.account_number_to),
                         int(r.currency_code), r.country, r.status, r.transaction_type,
                         int(r.amount), r.transaction_dt.to_pydatetime(), r.transaction_date)
                        for r in tx.itertuples(index=False)
                    ], 
                    page_size=2000
                )

            c.execute("DELETE FROM staging.currencies WHERE date_update = %s", (ds,))
            if not cur_day.empty:
                execute_values(
                    c, 
                    "INSERT INTO staging.currencies "
                    "(currency_code, currency_code_with, date_update, currency_with_div) VALUES %s",
                    [
                        (int(r.currency_code), int(r.currency_code_with), r.date_update,
                         float(r.currency_with_div)) 
                        for r in cur_day.itertuples(index=False)
                    ], 
                    page_size=1000
                )
        conn.commit()
    finally:
        conn.close()

    msg = f"day {ds}: tx={len(tx)}, rates={len(cur_day)}"
    log.info(msg)
    return msg


with DAG(
    dag_id="s3_to_staging_dag",
    start_date=datetime(2022, 10, 1),
    end_date=datetime(2022, 10, 31),
    schedule_interval="@daily",
    catchup=False,
    default_args={"owner": "s14905573", "retries": 2},
    tags=["s3", "staging", "postgres"],
) as dag:
    
    PythonOperator(
        task_id="load_day_to_staging", 
        python_callable=load_day
    )
