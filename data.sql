DROP TABLE IF EXISTS repayments CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS loans CASCADE;
DROP TABLE IF EXISTS customers CASCADE;


CREATE TABLE customers (
    customer_id        INTEGER PRIMARY KEY,
    full_name          VARCHAR(150) NOT NULL,
    gender             VARCHAR(10) NOT NULL
                       CHECK (gender IN ('Male','Female')),
    age                SMALLINT NOT NULL
                       CHECK (age BETWEEN 16 AND 100),
    region             VARCHAR(50) NOT NULL,
    signup_date        DATE NOT NULL,
    monthly_income     NUMERIC(12,2),
    employment_status  VARCHAR(30),
    credit_score       SMALLINT
                       CHECK (credit_score BETWEEN 300 AND 850),
    age_band           VARCHAR(10)
);


CREATE TABLE loans (
    loan_id             INTEGER PRIMARY KEY,
    customer_id         INTEGER NOT NULL
                        REFERENCES customers(customer_id),
    loan_product        VARCHAR(80) NOT NULL,
    loan_amount         NUMERIC(14,2) NOT NULL
                        CHECK (loan_amount > 0),
    interest_rate       NUMERIC(5,2) NOT NULL
                        CHECK (interest_rate >= 0),
    loan_date           DATE NOT NULL,
    due_date            DATE NOT NULL,
    status              VARCHAR(20) NOT NULL
                        CHECK (status IN ('active','closed','defaulted')),
    loan_purpose        VARCHAR(50),
    collateral_type     VARCHAR(50),
    collateral_value    NUMERIC(14,2),
    risk_flag           VARCHAR(10) NOT NULL
                        DEFAULT 'low'
                        CHECK (risk_flag IN ('low','high')),
    CHECK (due_date >= loan_date)
);


CREATE TABLE repayments (
    repayment_id        INTEGER PRIMARY KEY,
    loan_id             INTEGER NOT NULL
                        REFERENCES loans(loan_id),
    amount_paid         NUMERIC(14,2) NOT NULL
                        CHECK (amount_paid > 0),
    payment_date        DATE NOT NULL,
    principal_component NUMERIC(14,2) NOT NULL
                        CHECK (principal_component >= 0),
    interest_component  NUMERIC(14,2) NOT NULL
                        CHECK (interest_component >= 0),
    CHECK (
        principal_component + interest_component = amount_paid
    )
);


CREATE TABLE transactions (
    transaction_id   INTEGER PRIMARY KEY,
    customer_id      INTEGER NOT NULL
                     REFERENCES customers(customer_id),
    amount           NUMERIC(14,2) NOT NULL
                     CHECK (amount > 0),
    transaction_type VARCHAR(20) NOT NULL
                     CHECK (transaction_type IN ('deposit','withdrawal')),
    transaction_date DATE NOT NULL
);


CREATE INDEX idx_loans_customer_id
    ON loans(customer_id);

CREATE INDEX idx_loans_status
    ON loans(status);

CREATE INDEX idx_repayments_loan_id
    ON repayments(loan_id);

CREATE INDEX idx_txn_customer_id
    ON transactions(customer_id);

CREATE INDEX idx_customers_region
    ON customers(region);



SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN (
      'customers',
      'loans',
      'repayments',
      'transactions'
  )
ORDER BY table_name;    


SELECT
    current_database() AS database_name,
    current_schema() AS schema_name;

SELECT 'customers' AS tbl, count(*) FROM customers
UNION ALL
SELECT 'loans', count(*) FROM loans
UNION ALL
SELECT 'repayments', count(*) FROM repayments
UNION ALL
SELECT 'transactions', count(*) FROM transactions;


UPDATE customers
SET age_band = CASE 
    WHEN age BETWEEN 18 AND 24 THEN '18-24'
    WHEN age BETWEEN 25 AND 34 THEN '25-34'
    WHEN age BETWEEN 35 AND 49 THEN '35-49'
    WHEN age >= 50 THEN '50+'
    ELSE 'Unknown'
END;





SELECT 'customers' AS tbl, count(*) FROM customers

