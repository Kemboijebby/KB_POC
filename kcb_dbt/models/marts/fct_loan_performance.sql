{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['loan_id']
) }}

WITH loan_base AS (
    SELECT * FROM {{ ref('stg_loans') }}
),

repayment_agg AS (
    SELECT
        loan_id,
        count(repayment_id) AS total_repayments_count,
        sum(amount_paid) AS total_amount_paid,
        sum(principal_component) AS total_principal_paid,
        sum(interest_component) AS total_interest_paid,
        max(payment_date) AS last_payment_date
    FROM {{ ref('stg_repayments') }}
    GROUP BY loan_id
)

SELECT
    l.loan_id,
    l.customer_id,
    l.loan_product,
    l.loan_amount,
    l.interest_rate,
    l.loan_date,
    l.due_date,
    l.status AS reported_status,
    l.loan_purpose,
    l.collateral_type,
    l.collateral_value,
    coalesce(r.total_repayments_count, 0) AS total_repayments_count,
    coalesce(r.total_amount_paid, 0) AS total_amount_paid,
    coalesce(r.total_principal_paid, 0) AS total_principal_paid,
    coalesce(r.total_interest_paid, 0) AS total_interest_paid,
    (l.loan_amount - coalesce(r.total_principal_paid, 0)) AS outstanding_principal,
    r.last_payment_date
FROM loan_base l
LEFT JOIN repayment_agg r ON l.loan_id = r.loan_id