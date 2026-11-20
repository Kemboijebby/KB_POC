{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['customer_id']
) }}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

loan_metrics AS (
    SELECT
        customer_id,
        count(loan_id) AS total_loans_taken,
        sum(loan_amount) AS total_borrowed_amount,
        sum(outstanding_principal) AS current_total_debt,
        countIf(reported_status = 'Defaulted') AS defaulted_loans_count
    FROM {{ ref('fct_loan_performance') }}
    GROUP BY customer_id
),

tx_metrics AS (
    SELECT
        customer_id,
        count(transaction_id) AS total_transactions,
        sum(amount) AS total_transaction_volume,
        avg(amount) AS avg_transaction_value
    FROM {{ ref('stg_transactions') }}
    GROUP BY customer_id
)

SELECT
    customers.customer_id AS customer_id,
    customers.full_name AS full_name,
    customers.gender AS gender,
    customers.age AS age,
    customers.region AS region,
    customers.signup_date AS signup_date,
    customers.monthly_income AS monthly_income,
    customers.employment_status AS employment_status,
    customers.credit_score AS credit_score,
    customers.age_band AS age_band,
    coalesce(loan_metrics.total_loans_taken, 0) AS total_loans_taken,
    coalesce(loan_metrics.total_borrowed_amount, 0) AS total_borrowed_amount,
    coalesce(loan_metrics.current_total_debt, 0) AS current_total_debt,
    coalesce(loan_metrics.defaulted_loans_count, 0) AS defaulted_loans_count,
    coalesce(tx_metrics.total_transactions, 0) AS total_transactions,
    coalesce(tx_metrics.total_transaction_volume, 0) AS total_transaction_volume,
    coalesce(tx_metrics.avg_transaction_value, 0) AS avg_transaction_value,
    CASE 
        WHEN customers.credit_score < 580 OR coalesce(loan_metrics.defaulted_loans_count, 0) > 0 THEN 'High Risk'
        WHEN customers.credit_score BETWEEN 580 AND 669 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS risk_tier
FROM customers
LEFT JOIN loan_metrics USING (customer_id)
LEFT JOIN tx_metrics USING (customer_id)