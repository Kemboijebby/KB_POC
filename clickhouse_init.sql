CREATE DATABASE IF NOT EXISTS kcb_analytics;

-- =========================================================
-- 1. CUSTOMERS PIPELINE
-- =========================================================
DROP TABLE IF EXISTS kcb_analytics.mv_customers;
DROP TABLE IF EXISTS kcb_analytics.kafka_customers;
DROP TABLE IF EXISTS kcb_analytics.customers;

CREATE TABLE kcb_analytics.customers (
    customer_id UInt64,
    full_name String,
    gender LowCardinality(String),
    age UInt8,
    region LowCardinality(String),
    signup_date Date,
    monthly_income Decimal(12, 2),
    employment_status LowCardinality(String),
    credit_score UInt16,
    age_band LowCardinality(String)
) ENGINE = ReplacingMergeTree()
ORDER BY customer_id;

CREATE TABLE kcb_analytics.kafka_customers (
    payload Tuple(
        after Tuple(
            customer_id UInt64,
            full_name String,
            gender String,
            age UInt8,
            region String,
            signup_date Int32,
            monthly_income Float64,
            employment_status String,
            credit_score UInt16,
            age_band String
        ),
        op String
    )
) ENGINE = Kafka
SETTINGS 
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'kcbv2.public.customers',
    kafka_group_name = 'ch_customers_v7',
    kafka_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW kcb_analytics.mv_customers TO kcb_analytics.customers AS
SELECT
    payload.after.customer_id AS customer_id,
    payload.after.full_name AS full_name,
    payload.after.gender AS gender,
    payload.after.age AS age,
    payload.after.region AS region,
    toDate(payload.after.signup_date) AS signup_date,
    toDecimal64(payload.after.monthly_income, 2) AS monthly_income,
    payload.after.employment_status AS employment_status,
    payload.after.credit_score AS credit_score,
    payload.after.age_band AS age_band
FROM kcb_analytics.kafka_customers
WHERE payload.op IN ('c', 'r', 'u');


-- =========================================================
-- 2. LOANS PIPELINE
-- =========================================================
DROP TABLE IF EXISTS kcb_analytics.mv_loans;
DROP TABLE IF EXISTS kcb_analytics.kafka_loans;
DROP TABLE IF EXISTS kcb_analytics.loans;

CREATE TABLE kcb_analytics.loans (
    loan_id UInt64,
    customer_id UInt64,
    loan_product LowCardinality(String),
    loan_amount Decimal(14, 2),
    interest_rate Decimal(5, 2),
    loan_date Date,
    due_date Date,
    status LowCardinality(String),
    loan_purpose LowCardinality(String),
    collateral_type LowCardinality(String),
    collateral_value Decimal(14, 2)
) ENGINE = ReplacingMergeTree()
ORDER BY loan_id;

CREATE TABLE kcb_analytics.kafka_loans (
    payload Tuple(
        after Tuple(
            loan_id UInt64,
            customer_id UInt64,
            loan_product String,
            loan_amount Float64,
            interest_rate Float64,
            loan_date Int32,
            due_date Int32,
            status String,
            loan_purpose String,
            collateral_type String,
            collateral_value Float64
        ),
        op String
    )
) ENGINE = Kafka
SETTINGS 
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'kcbv2.public.loans',
    kafka_group_name = 'ch_loans_v7',
    kafka_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW kcb_analytics.mv_loans TO kcb_analytics.loans AS
SELECT
    payload.after.loan_id AS loan_id,
    payload.after.customer_id AS customer_id,
    payload.after.loan_product AS loan_product,
    toDecimal64(payload.after.loan_amount, 2) AS loan_amount,
    toDecimal64(payload.after.interest_rate, 2) AS interest_rate,
    toDate(payload.after.loan_date) AS loan_date,
    toDate(payload.after.due_date) AS due_date,
    payload.after.status AS status,
    payload.after.loan_purpose AS loan_purpose,
    payload.after.collateral_type AS collateral_type,
    toDecimal64(payload.after.collateral_value, 2) AS collateral_value
FROM kcb_analytics.kafka_loans
WHERE payload.op IN ('c', 'r', 'u');


-- =========================================================
-- 3. REPAYMENTS PIPELINE
-- =========================================================
DROP TABLE IF EXISTS kcb_analytics.mv_repayments;
DROP TABLE IF EXISTS kcb_analytics.kafka_repayments;
DROP TABLE IF EXISTS kcb_analytics.repayments;

CREATE TABLE kcb_analytics.repayments (
    repayment_id UInt64,
    loan_id UInt64,
    amount_paid Decimal(14, 2),
    payment_date Date,
    principal_component Decimal(14, 2),
    interest_component Decimal(14, 2)
) ENGINE = ReplacingMergeTree()
ORDER BY repayment_id;

CREATE TABLE kcb_analytics.kafka_repayments (
    payload Tuple(
        after Tuple(
            repayment_id UInt64,
            loan_id UInt64,
            amount_paid Float64,
            payment_date Int32,
            principal_component Float64,
            interest_component Float64
        ),
        op String
    )
) ENGINE = Kafka
SETTINGS 
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'kcbv2.public.repayments',
    kafka_group_name = 'ch_repayments_v7',
    kafka_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW kcb_analytics.mv_repayments TO kcb_analytics.repayments AS
SELECT
    payload.after.repayment_id AS repayment_id,
    payload.after.loan_id AS loan_id,
    toDecimal64(payload.after.amount_paid, 2) AS amount_paid,
    toDate(payload.after.payment_date) AS payment_date,
    toDecimal64(payload.after.principal_component, 2) AS principal_component,
    toDecimal64(payload.after.interest_component, 2) AS interest_component
FROM kcb_analytics.kafka_repayments
WHERE payload.op IN ('c', 'r', 'u');


-- =========================================================
-- 4. TRANSACTIONS PIPELINE
-- =========================================================
DROP TABLE IF EXISTS kcb_analytics.mv_transactions;
DROP TABLE IF EXISTS kcb_analytics.kafka_transactions;
DROP TABLE IF EXISTS kcb_analytics.transactions;

CREATE TABLE kcb_analytics.transactions (
    transaction_id UInt64,
    customer_id UInt64,
    amount Decimal(14, 2),
    transaction_type LowCardinality(String),
    transaction_date Date
) ENGINE = ReplacingMergeTree()
ORDER BY transaction_id;

CREATE TABLE kcb_analytics.kafka_transactions (
    payload Tuple(
        after Tuple(
            transaction_id UInt64,
            customer_id UInt64,
            amount Float64,
            transaction_type String,
            transaction_date Int32
        ),
        op String
    )
) ENGINE = Kafka
SETTINGS 
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'kcbv2.public.transactions',
    kafka_group_name = 'ch_transactions_v7',
    kafka_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW kcb_analytics.mv_transactions TO kcb_analytics.transactions AS
SELECT
    payload.after.transaction_id AS transaction_id,
    payload.after.customer_id AS customer_id,
    toDecimal64(payload.after.amount, 2) AS amount,
    payload.after.transaction_type AS transaction_type,
    toDate(payload.after.transaction_date) AS transaction_date
FROM kcb_analytics.kafka_transactions
WHERE payload.op IN ('c', 'r', 'u');