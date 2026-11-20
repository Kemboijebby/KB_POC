{{ config(materialized='view') }}

SELECT
    transaction_id,
    customer_id,
    amount,
    transaction_type,
    transaction_date
FROM {{ source('kcb_raw', 'transactions') }} FINAL