{{ config(materialized='view') }}

SELECT
    loan_id,
    customer_id,
    loan_product,
    loan_amount,
    interest_rate,
    loan_date,
    due_date,
    status,
    loan_purpose,
    collateral_type,
    collateral_value
FROM {{ source('kcb_raw', 'loans') }} FINAL