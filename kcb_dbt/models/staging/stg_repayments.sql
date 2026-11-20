{{ config(materialized='view') }}

SELECT
    repayment_id,
    loan_id,
    amount_paid,
    payment_date,
    principal_component,
    interest_component
FROM {{ source('kcb_raw', 'repayments') }} FINAL