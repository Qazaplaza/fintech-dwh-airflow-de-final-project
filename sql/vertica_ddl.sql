-- Применить после восстановления Vertica.

CREATE TABLE IF NOT EXISTS VT2608212D19C6__STAGING.transactions (
    operation_id VARCHAR(64) NOT NULL,
    account_number_from INTEGER NOT NULL,
    account_number_to INTEGER NOT NULL,
    currency_code INTEGER NOT NULL,
    country VARCHAR(64),
    status VARCHAR(32),
    transaction_type VARCHAR(64),
    amount INTEGER,
    transaction_dt TIMESTAMP,
    transaction_date DATE NOT NULL
) 
ORDER BY transaction_date, operation_id
SEGMENTED BY HASH(transaction_date, operation_id) ALL NODES;

-- Проекция по дате (требование методички)
CREATE PROJECTION IF NOT EXISTS VT2608212D19C6__STAGING.transactions_date_proj AS
    SELECT * FROM VT2608212D19C6__STAGING.transactions
    ORDER BY transaction_date, operation_id
    SEGMENTED BY HASH(transaction_date, operation_id) ALL NODES;

CREATE TABLE IF NOT EXISTS VT2608212D19C6__STAGING.currencies (
    currency_code INTEGER NOT NULL,
    currency_code_with INTEGER NOT NULL,
    date_update DATE NOT NULL,
    currency_with_div NUMERIC(18,6)
) 
ORDER BY date_update, currency_code
SEGMENTED BY HASH(date_update, currency_code) ALL NODES;

CREATE TABLE IF NOT EXISTS VT2608212D19C6__DWH.global_metrics (
    date_update DATE NOT NULL,
    currency_from INTEGER NOT NULL,
    amount_total NUMERIC(18,2),
    cnt_transactions INTEGER,
    avg_transactions_per_account NUMERIC(18,4),
    cnt_accounts_make_transactions INTEGER
) 
ORDER BY date_update, currency_from
SEGMENTED BY HASH(date_update, currency_from) ALL NODES;
