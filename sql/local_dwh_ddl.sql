-- 1. Схемы
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;

-- 2. Staging: сырые транзакции
CREATE TABLE IF NOT EXISTS staging.transactions (
    operation_id VARCHAR(64) NOT NULL,
    account_number_from BIGINT NOT NULL,
    account_number_to BIGINT NOT NULL,
    currency_code INTEGER NOT NULL,
    country VARCHAR(64),
    status VARCHAR(32),
    transaction_type VARCHAR(64),
    amount BIGINT,
    transaction_dt TIMESTAMP,
    transaction_date DATE NOT NULL,
    PRIMARY KEY (operation_id, transaction_date)
);
CREATE INDEX IF NOT EXISTS ix_stg_tx_date ON staging.transactions (transaction_date);

-- 3. Staging: курсы валют
CREATE TABLE IF NOT EXISTS staging.currencies (
    currency_code INTEGER NOT NULL,
    currency_code_with INTEGER NOT NULL,
    date_update DATE NOT NULL,
    currency_with_div NUMERIC(18,6),
    PRIMARY KEY (currency_code, currency_code_with, date_update)
);

-- 4. Витрина: global_metrics
CREATE TABLE IF NOT EXISTS dwh.global_metrics (
    date_update DATE NOT NULL,
    currency_from INTEGER NOT NULL,
    amount_total NUMERIC(18,2),
    cnt_transactions INTEGER,
    avg_transactions_per_account NUMERIC(18,4),
    cnt_accounts_make_transactions INTEGER,
    PRIMARY KEY (date_update, currency_from)
);

-- 5. Проверка: что таблицы созданы
SELECT 
    table_schema, 
    table_name
FROM information_schema.tables
WHERE table_schema IN ('staging', 'dwh')
ORDER BY 1, 2;
