import io
import logging
from datetime import datetime

import boto3
import pandas as pd
import vertica_python
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# --- Конфигурация ---
S3_ENDPOINT = "https://storage.yandexcloud.net"
BUCKET = "final-project"
BATCHES = [f"transactions_batch_{i}.csv" for i in range(1, 11)]

STG_SCHEMA = "VT2608212D19C6__STAGING"
TX_COLS = [
    "operation_id", "account_number_from", "account_number_to", "currency_code",
    "country", "status", "transaction_type", "amount", "transaction_dt", "transaction_date"
]
CUR_COLS = ["currency_code", "currency_code_with", "date_update", "currency_with_div"]

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=Variable.get("s3_access_key", default_var=""),
        aws_secret_access_key=Variable.get("s3_secret_key", default_var=""),
    )

def get_vertica_conn():
    return vertica_python.connect(
        host=Variable.get("vertica_host"),
        port=int(Variable.get("vertica_port")),
        user=Variable.get("vertica_user"),
        password=Variable.get("vertica_password"),
        database=Variable.get("vertica_db"),
    )

def load_day(ds, **kwargs):
    """
    Читает батчи из S3, фильтрует по дате ds и идемпотентно 
    загружает в staging-слой Vertica через COPY.
    """
    s3 = get_s3_client()
    
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
            
    tx = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=TX_COLS)
    tx = tx.drop_duplicates()

    cur_obj = s3.get_object(Bucket=BUCKET, Key="currencies_history.csv")
    cur_df = pd.read_csv(io.BytesIO(cur_obj["Body"].read()))
    cur_df["date_update"] = pd.to_datetime(cur_df["date_update"]).dt.strftime("%Y-%m-%d")
    cur_day = cur_df[cur_df["date_update"] == ds]

    conn = get_vertica_conn()
    try:
        c = conn.cursor()
        
        # Очистка и загрузка транзакций
        c.execute(f"DELETE FROM {STG_SCHEMA}.transactions WHERE transaction_date = DATE '{ds}'")
        if not tx.empty:
            buf = io.StringIO()
            tx[TX_COLS].to_csv(buf, index=False, header=False)
            buf.seek(0)
            c.copy(
                f"COPY {STG_SCHEMA}.transactions ({', '.join(TX_COLS)}) "
                f"FROM STDIN DELIMITER ',' ENCLOSED BY '\"'", 
                buf
            )
            
        # Очистка и загрузка курсов
        c.execute(f"DELETE FROM {STG_SCHEMA}.currencies WHERE date_update = DATE '{ds}'")
        if not cur_day.empty:
            buf = io.StringIO()
            cur_day[CUR_COLS].to_csv(buf, index=False, header=False)
            buf.seek(0)
            c.copy(
                f"COPY {STG_SCHEMA}.currencies ({', '.join(CUR_COLS)}) "
                f"FROM STDIN DELIMITER ',' ENCLOSED BY '\"'", 
                buf
            )
            
        conn.commit()
    finally:
        conn.close()
        
    msg = f"day {ds}: tx={len(tx)}, rates={len(cur_day)}"
    log.info(msg)
    return msg


with DAG(
    dag_id="s3_to_vertica_staging_dag",
    start_date=datetime(2022, 10, 1),
    end_date=datetime(2022, 10, 31),
    schedule_interval="@daily",
    catchup=False,
    default_args={"owner": "s14905573", "retries": 2},
    tags=["s3", "staging", "vertica"],
) as dag:
    
    PythonOperator(
        task_id="load_day_to_vertica", 
        python_callable=load_day
    )
