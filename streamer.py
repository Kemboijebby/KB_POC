"""
KCB synthetic banking event generator (v2)

Writes live INSERT and UPDATE activity into PostgreSQL so Debezium has real
change events to capture.

What it generates each tick:
  - transactions   (most frequent)
  - repayments     (against active loans, with principal/interest split)
  - new loans      (occasional)
  - loan UPDATEs   (active -> closed / defaulted)
  - new customers  (rare)
  - customer UPDATEs (credit score refresh)

Volume is controlled with --scale (multiplies ticks per cycle) and --interval.

Install:   pip install psycopg2-binary faker
Run:       python stream_data_generator_v2.py
Fast:      python stream_data_generator_v2.py --scale 20 --interval 1
Test:      python stream_data_generator_v2.py --once
"""

import argparse
import datetime as dt
import random
import sys
import time

import psycopg2
from faker import Faker

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "kcb_db",
    "user": "postgres",
    "password": "admin01",
}

# ---------------------------------------------------------------------------
# Names come from Faker's Kenyan-English locale (en_KE).
# Business facts (regions, products, collateral rules) stay as explicit rules.
# ---------------------------------------------------------------------------
try:
    fake = Faker("en_KE")
except AttributeError:
    sys.exit("This Faker version has no en_KE locale. Run: pip install --upgrade faker")


def kenyan_full_name(gender):
    # Built from first + last name directly so we never get titles
    # ("Dr.", "Mr.") or suffixes inside full_name.
    first = fake.first_name_male() if gender == "Male" else fake.first_name_female()
    return f"{first} {fake.last_name()}"


REGIONS = [
    "Nairobi", "Mombasa", "Kisumu", "Nakuru", "Eldoret", "Nyeri", "Meru",
    "Kakamega", "Machakos", "Garissa", "Kisii", "Thika", "Nyahururu",
    "Kericho", "Malindi",
]
REGION_WEIGHTS = [22, 12, 9, 8, 7, 5, 5, 6, 6, 3, 5, 5, 3, 4, 5]

# (name, weight, amount range, rate range, tenure months range)
# Names match the historical dataset exactly so group-bys don't split.
LOAN_PRODUCTS = [
    ("Personal Loan (Unsecured)", 34, (50_000, 2_000_000), (15.0, 19.0), (6, 60)),
    ("Check-off Loan (Salaried/Civil Servant)", 18, (100_000, 3_000_000), (14.9, 16.99), (12, 72)),
    ("SME / Business Loan", 16, (500_000, 10_000_000), (13.0, 17.0), (12, 84)),
    ("Asset & Vehicle Finance", 12, (800_000, 6_000_000), (12.0, 16.0), (12, 60)),
    ("Home Loan / Mortgage", 12, (1_000_000, 15_000_000), (8.9, 12.5), (60, 300)),
    ("Affordable Housing Mortgage (MSME/Informal)", 8, (1_000_000, 4_000_000), (9.0, 9.9), (60, 180)),
]

# employment status, weight, monthly income range (KES), credit score (mean, std)
EMPLOYMENT = [
    ("Employed", 35, (30_000, 250_000), (640, 70)),
    ("Government Employee", 15, (25_000, 180_000), (660, 60)),
    ("Self-Employed", 25, (15_000, 300_000), (600, 90)),
    ("Business Owner", 20, (20_000, 500_000), (610, 95)),
    ("Unemployed", 5, (0, 15_000), (520, 80)),
]

PURPOSES = [
    "Business Expansion", "Education", "Emergency/Medical", "Home Improvement",
    "Asset Purchase", "Debt Consolidation", "Working Capital", "Personal/Other",
]
PURPOSE_WEIGHTS = [18, 14, 12, 14, 16, 10, 10, 6]


def weighted_choice(items, weights):
    return random.choices(items, weights=weights, k=1)[0]


def collateral_for_product(product, amount):
    if product == "Asset & Vehicle Finance":
        return "Vehicle", round(amount * random.uniform(1.0, 1.4), -3)
    if product in ("Home Loan / Mortgage", "Affordable Housing Mortgage (MSME/Informal)"):
        return "Property", round(amount * random.uniform(1.0, 1.4), -3)
    if product == "SME / Business Loan":
        kind = random.choices(["Business Assets", "Property", "None"], weights=[45, 25, 30])[0]
        return kind, (round(amount * random.uniform(1.0, 1.4), -3) if kind != "None" else 0)
    return "None", 0


class StreamGenerator:
    ACTIVE_LOAN_CACHE = 3000
    ACTIVE_LOAN_REFILL_BELOW = 500

    def __init__(self, conn, scale=1):
        self.conn = conn
        self.cur = conn.cursor()
        self.scale = scale
        self.verbose = scale == 1   # per-event logging only at low volume
        self._load_state()

    # -----------------------------------------------------------------------
    # State: continue IDs from the existing dataset, and detect which columns
    # each table actually has so inserts adapt to whichever schema is live.
    # -----------------------------------------------------------------------
    def _table_columns(self, table):
        self.cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = %s",
            (table,),
        )
        return {r[0] for r in self.cur.fetchall()}

    def _load_state(self):
        self.columns = {
            t: self._table_columns(t)
            for t in ("customers", "loans", "repayments", "transactions")
        }

        def next_id(table, col):
            self.cur.execute(f"SELECT COALESCE(MAX({col}), 0) FROM {table}")
            return self.cur.fetchone()[0] + 1

        self.next_customer_id = next_id("customers", "customer_id")
        self.next_loan_id = next_id("loans", "loan_id")
        self.next_repayment_id = next_id("repayments", "repayment_id")
        self.next_transaction_id = next_id("transactions", "transaction_id")

        self.cur.execute(
            "SELECT customer_id FROM customers ORDER BY random() LIMIT 3000"
        )
        self.customer_ids = [r[0] for r in self.cur.fetchall()]

        self.active_loans = {}
        self._refill_active_loans()

        extras = sorted(
            (self.columns["customers"] & {"monthly_income", "employment_status", "credit_score"})
            | (self.columns["loans"] & {"loan_purpose", "collateral_type", "collateral_value"})
            | (self.columns["repayments"] & {"principal_component", "interest_component"})
        )
        print(
            f"State loaded | next ids: customer={self.next_customer_id}, "
            f"loan={self.next_loan_id}, repayment={self.next_repayment_id}, "
            f"transaction={self.next_transaction_id}"
        )
        print(f"Cached {len(self.customer_ids)} customers, {len(self.active_loans)} active loans")
        print(f"Insight columns detected and will be populated: {extras or 'none'}")

    def _refill_active_loans(self):
        self.cur.execute(
            "SELECT loan_id, loan_amount, interest_rate FROM loans "
            "WHERE status = 'active' ORDER BY random() LIMIT %s",
            (self.ACTIVE_LOAN_CACHE,),
        )
        self.active_loans = {
            r[0]: {"amount": float(r[1]), "rate": float(r[2])}
            for r in self.cur.fetchall()
        }

    def _insert(self, table, row):
        # Only write columns that exist in the live table; anything we don't
        # supply (e.g. a risk_flag default) is left to PostgreSQL.
        cols = [c for c in row if c in self.columns[table]]
        placeholders = ", ".join(["%s"] * len(cols))
        self.cur.execute(
            f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})",
            [row[c] for c in cols],
        )

    def _log(self, msg):
        if self.verbose:
            print(msg)

    # -----------------------------------------------------------------------
    # CUSTOMERS
    # -----------------------------------------------------------------------
    def new_customer(self):
        customer_id = self.next_customer_id
        self.next_customer_id += 1

        gender = random.choice(["Male", "Female"])
        full_name = kenyan_full_name(gender)
        region = weighted_choice(REGIONS, REGION_WEIGHTS)

        status, _, income_range, (score_mean, score_std) = EMPLOYMENT[
            random.choices(range(len(EMPLOYMENT)), weights=[e[1] for e in EMPLOYMENT])[0]
        ]
        row = {
            "customer_id": customer_id,
            "full_name": full_name,
            "gender": gender,
            "age": random.randint(19, 70),
            "region": region,
            "signup_date": dt.date.today(),
            "monthly_income": round(random.uniform(*income_range), -2),
            "employment_status": status,
            "credit_score": int(min(850, max(300, random.gauss(score_mean, score_std)))),
        }
        self._insert("customers", row)
        self.customer_ids.append(customer_id)
        self._log(f"[customer] INSERT #{customer_id} | {full_name} | {status} | {region}")

    def update_customer_credit_score(self):
        # A second source of UPDATE events: scores get refreshed over time.
        if "credit_score" not in self.columns["customers"] or not self.customer_ids:
            return
        customer_id = random.choice(self.customer_ids)
        delta = random.randint(-25, 25)
        self.cur.execute(
            "UPDATE customers SET credit_score = LEAST(850, GREATEST(300, credit_score + %s)) "
            "WHERE customer_id = %s",
            (delta, customer_id),
        )
        self._log(f"[customer] UPDATE #{customer_id} | credit_score {delta:+d}")

    # -----------------------------------------------------------------------
    # LOANS
    # -----------------------------------------------------------------------
    def new_loan(self):
        if not self.customer_ids:
            return
        loan_id = self.next_loan_id
        self.next_loan_id += 1

        customer_id = random.choice(self.customer_ids)
        name, _, amt_range, rate_range, tenure_range = LOAN_PRODUCTS[
            random.choices(range(len(LOAN_PRODUCTS)), weights=[p[1] for p in LOAN_PRODUCTS])[0]
        ]
        amount = round(random.uniform(*amt_range), -3)
        rate = round(random.uniform(*rate_range), 2)
        loan_date = dt.date.today()
        due_date = loan_date + dt.timedelta(days=int(random.randint(*tenure_range) * 30.4))
        collateral_type, collateral_value = collateral_for_product(name, amount)

        row = {
            "loan_id": loan_id,
            "customer_id": customer_id,
            "loan_product": name,
            "loan_amount": amount,
            "interest_rate": rate,
            "loan_date": loan_date,
            "due_date": due_date,
            "status": "active",
            "loan_purpose": weighted_choice(PURPOSES, PURPOSE_WEIGHTS),
            "collateral_type": collateral_type,
            "collateral_value": collateral_value,
        }
        self._insert("loans", row)
        self.active_loans[loan_id] = {"amount": amount, "rate": rate}
        self._log(f"[loan] INSERT #{loan_id} | {name} | KES {amount:,.0f} | customer #{customer_id}")

    def update_loan_status(self):
        if not self.active_loans:
            return
        loan_id = random.choice(list(self.active_loans))
        new_status = random.choices(["closed", "defaulted"], weights=[80, 20])[0]
        self.cur.execute(
            "UPDATE loans SET status = %s WHERE loan_id = %s", (new_status, loan_id)
        )
        del self.active_loans[loan_id]
        self._log(f"[loan] UPDATE #{loan_id} | status -> {new_status}")

    # -----------------------------------------------------------------------
    # REPAYMENTS
    # -----------------------------------------------------------------------
    def new_repayment(self):
        if not self.active_loans:
            return
        loan_id = random.choice(list(self.active_loans))
        loan = self.active_loans[loan_id]

        repayment_id = self.next_repayment_id
        self.next_repayment_id += 1

        installment = round(loan["amount"] * random.uniform(0.02, 0.08), 2)
        # Higher-rate loans carry a larger interest share per installment.
        interest_share = min(0.45, max(0.08, 0.08 + (loan["rate"] / 100.0) * 1.6
                                       + random.uniform(-0.05, 0.05)))
        interest = round(installment * interest_share, 2)

        row = {
            "repayment_id": repayment_id,
            "loan_id": loan_id,
            "amount_paid": installment,
            "payment_date": dt.date.today(),
            "principal_component": round(installment - interest, 2),
            "interest_component": interest,
        }
        self._insert("repayments", row)
        self._log(f"[repayment] INSERT #{repayment_id} | KES {installment:,.0f} | loan #{loan_id}")

    # -----------------------------------------------------------------------
    # TRANSACTIONS
    # -----------------------------------------------------------------------
    def new_transaction(self):
        if not self.customer_ids:
            return
        transaction_id = self.next_transaction_id
        self.next_transaction_id += 1

        txn_type = random.choices(["deposit", "withdrawal"], weights=[55, 45])[0]
        amount = round(random.uniform(500, 850_000), 2) if txn_type == "deposit" \
            else round(random.uniform(200, 400_000), 2)

        row = {
            "transaction_id": transaction_id,
            "customer_id": random.choice(self.customer_ids),
            "amount": amount,
            "transaction_type": txn_type,
            "transaction_date": dt.date.today(),
        }
        self._insert("transactions", row)
        self._log(f"[transaction] INSERT #{transaction_id} | {txn_type} | KES {amount:,.2f}")

    # -----------------------------------------------------------------------
    # One streaming cycle = `scale` ticks, then a single commit
    # -----------------------------------------------------------------------
    def run_cycle(self):
        if len(self.active_loans) < self.ACTIVE_LOAN_REFILL_BELOW:
            self._refill_active_loans()

        n_txn = n_rep = 0
        for _ in range(self.scale):
            for _ in range(random.randint(3, 8)):
                self.new_transaction()
                n_txn += 1
            for _ in range(random.randint(0, 3)):
                self.new_repayment()
                n_rep += 1
            if random.random() < 0.15:
                self.new_loan()
            if random.random() < 0.05:
                self.update_loan_status()
            if random.random() < 0.05:
                self.new_customer()
            if random.random() < 0.05:
                self.update_customer_credit_score()

        self.conn.commit()
        print(
            f"[{dt.datetime.now().strftime('%H:%M:%S')}] COMMIT | "
            f"{n_txn} transactions, {n_rep} repayments"
        )


def main():
    parser = argparse.ArgumentParser(description="KCB synthetic banking event generator")
    parser.add_argument("--interval", type=float, default=5.0,
                        help="seconds between cycles (default: 5)")
    parser.add_argument("--scale", type=int, default=1,
                        help="multiplier on events per cycle (default: 1)")
    parser.add_argument("--once", action="store_true", help="run one cycle and exit")
    args = parser.parse_args()

    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except psycopg2.OperationalError as exc:
        print(f"Could not connect to PostgreSQL: {exc}")
        print("Check DB_CONFIG at the top of the script.")
        sys.exit(1)

    generator = StreamGenerator(conn, scale=max(1, args.scale))

    if args.once:
        generator.run_cycle()
        conn.close()
        return

    print(f"Streaming started: scale={args.scale}, one cycle every {args.interval}s. Ctrl+C to stop.")
    try:
        while True:
            generator.run_cycle()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStreaming stopped.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()