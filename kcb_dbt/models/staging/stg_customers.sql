{{ config(materialized='view') }}

SELECT
    customer_id,
    full_name,
    gender,
    age,
    region,
    signup_date,
    monthly_income,
    employment_status,
    credit_score,
    age_band
FROM {{ source('kcb_raw', 'customers') }} FINAL