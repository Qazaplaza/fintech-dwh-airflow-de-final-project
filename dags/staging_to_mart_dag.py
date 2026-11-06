import os
import logging
from datetime import datetime

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

PG_DSN = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": os.getenv("PG_PORT", "5432"),
    "dbname": os.getenv("PG_DB", "de"),
    "user": os.getenv("PG_USER", "jovyan"),
    "password": os.getenv("PG_PASSWORD", "jovyan"),
}
USD_CODE = 420  # Код USD из справочника

def build_mart(ds, **kwargs):
    """
    Рассчитывает агрегаты для витрины global_metrics за день ds 
    и идемпотентно обновляет таблицу в dwh.
    """
    sql = f"""
        DELETE FROM dwh.global_metrics WHERE date_update = %(ds)s;

        INSERT INTO dwh.global_metrics
            SELECT 
                t.transaction_date AS date_update,
                t.currency_code AS currency_from,
                SUM(t.amount * COALESCE(r.currency_with_div, 1)) / 100.0 AS amount_total,
                COUNT(*) AS cnt_transactions,
                COUNT(*)::float / NULLIF(COUNT(DISTINCT t.account_number_from), 0) AS avg_transactions_per_account,
                COUNT(DISTINCT t.account_number_from) AS cnt_accounts_make_transactions
        FROM 
            staging.transactions t
            LEFT JOIN staging.currencies r 
                ON r.currency_code = t.currency_code
                AND r.date_update = t.transaction_date
                AND r.currency_code_with = {USD_CODE}
        WHERE 
            t.transaction_date = %(ds)s
            AND t.status = 'done'                       -- только успешные
            AND t.account_number_from >= 0              -- очистка тестовых
            AND t.account_number_to   >= 0
        GROUP BY 1, 2;
    """
    
    conn = psycopg2.connect(**PG_DSN)
    try:
        with conn.cursor() as c:
            c.execute(sql, {"ds": ds})
            n = c.rowcount
        conn.commit()
    finally:
        conn.close()
        
    log.info("mart day %s: %s rows inserted", ds, n)
    return n


with DAG(
    dag_id="staging_to_mart_dag",
    start_date=datetime(2022, 10, 1),
    end_date=datetime(2022, 10, 31),
    schedule_interval="@daily",
    catchup=False,
    default_args={"owner": "s14905573", "retries": 2},
    tags=["mart", "postgres", "dwh"],
) as dag:
    
    PythonOperator(
        task_id="refresh_global_metrics", 
        python_callable=build_mart
    )
