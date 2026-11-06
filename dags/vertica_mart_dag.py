import logging
from datetime import datetime

import vertica_python
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

STG_SCHEMA = "VT2608212D19C6__STAGING"
DWH_SCHEMA = "VT2608212D19C6__DWH"
USD_CODE = 420  # Код USD из справочника

def get_vertica_conn():
    return vertica_python.connect(
        host=Variable.get("vertica_host"),
        port=int(Variable.get("vertica_port")),
        user=Variable.get("vertica_user"),
        password=Variable.get("vertica_password"),
        database=Variable.get("vertica_db"),
    )

def build_mart(ds, **kwargs):
    """
    Инкремент витрины за день ds: delete+insert, 
    вся агрегация выполняется на стороне Vertica.
    """
    conn = get_vertica_conn()
    try:
        c = conn.cursor()
        
        c.execute(f"""
            DELETE FROM {DWH_SCHEMA}.global_metrics 
            WHERE date_update = DATE '{ds}'
        """)
        
        c.execute(f"""
            INSERT INTO {DWH_SCHEMA}.global_metrics
            SELECT 
                t.transaction_date AS date_update,
                t.currency_code AS currency_from,
                SUM(t.amount * COALESCE(r.currency_with_div, 1)) / 100.0 AS amount_total,
                COUNT(*) AS cnt_transactions,
                COUNT(*)::float / NULLIF(COUNT(DISTINCT t.account_number_from), 0) AS avg_transactions_per_account,
                COUNT(DISTINCT t.account_number_from) AS cnt_accounts_make_transactions
            FROM 
                {STG_SCHEMA}.transactions t
                LEFT JOIN {STG_SCHEMA}.currencies r
                    ON r.currency_code = t.currency_code
                    AND r.date_update = t.transaction_date
                    AND r.currency_code_with = {USD_CODE}
            WHERE 
                t.transaction_date = DATE '{ds}'
                AND t.status = 'done'                       -- только завершённые операции
                AND t.account_number_from >= 0              -- очистка тестовых аккаунтов
                AND t.account_number_to >= 0
            GROUP BY 
                t.transaction_date, 
                t.currency_code
        """)
        conn.commit()
    finally:
        conn.close()
        
    log.info("mart day %s refreshed in Vertica", ds)


with DAG(
    dag_id="vertica_mart_dag",
    start_date=datetime(2022, 10, 1),
    end_date=datetime(2022, 10, 31),
    schedule_interval="@daily",
    catchup=False,
    default_args={"owner": "s14905573", "retries": 2},
    tags=["mart", "vertica", "dwh"],
) as dag:
    
    PythonOperator(
        task_id="refresh_global_metrics", 
        python_callable=build_mart
    )
