import psycopg2
import psycopg2.extras as extras
import pandas as pd
import numpy as np
from datetime import date
import config

# ── CONNECTION ─────────────────────────────────────────────────────────────────

def get_connection():
    return psycopg2.connect(**config.DB_CONFIG)


# ── CORE INSERT FUNCTION ───────────────────────────────────────────────────────

def bulk_insert(conn, df, table_name, columns):
    """
    Uses execute_values for fast bulk insert —
    much faster than inserting row by row.
    """
    df = df[columns].copy()

    # Convert numpy types to native Python — psycopg2 can't handle numpy types
    df = df.where(pd.notnull(df), None)  # NaN → None (NULL in Postgres)

    for col in df.select_dtypes(include=['int64', 'int32']).columns:
        df[col] = df[col].astype(object).where(df[col].notna()).apply(
            lambda x: int(x) if x is not None else None
        )
    for col in df.select_dtypes(include=['float64', 'float32']).columns:
        df[col] = df[col].astype(object).where(df[col].notna()).apply(
            lambda x: float(x) if x is not None else None
        )
    for col in df.select_dtypes(include=['bool']).columns:
        df[col] = df[col].astype(object).where(df[col].notna()).apply(
            lambda x: bool(x) if x is not None else None
        )

    tuples = [tuple(row) for row in df.itertuples(index=False)]
    cols   = ', '.join(columns)
    query  = f"INSERT INTO {table_name} ({cols}) VALUES %s"

    with conn.cursor() as cur:
        extras.execute_values(cur, query, tuples, page_size=500)
    conn.commit()
    print(f"  ✓ {table_name}: {len(df)} rows inserted")


# ── TABLE-SPECIFIC LOADERS ─────────────────────────────────────────────────────

def load_officers(conn, df):
    cols = ['officer_id', 'full_name', 'branch', 'region', 'joined_date']
    bulk_insert(conn, df, 'loan_officer', cols)


def load_applicants(conn, df):
    cols = [
        'applicant_id', 'full_name', 'age', 'gender', 'city', 'states',
        'employment_type', 'annual_income', 'education_level',
        'years_employed', 'marital_status'
    ]
    bulk_insert(conn, df, 'applicants', cols)


def load_bureau(conn, df):
    cols = [
        'bureau_id', 'applicant_id', 'credit_score', 'existing_loans_count',
        'total_existing_debt', 'missed_payments_count', 'bankruptcies',
        'bureau_pull_date'
    ]
    bulk_insert(conn, df, 'credit_bureau', cols)


def load_loans(conn, df):
    cols = [
        'loan_id', 'applicant_id', 'officer_id', 'loan_type', 'loan_amount',
        'interest_rate', 'tenure_months', 'application_date', 'approval_date',
        'loan_status', 'default_flag'
    ]
    bulk_insert(conn, df, 'loans', cols)


def load_repayments(conn, df):
    cols = [
        'repayment_id', 'loan_id', 'due_date', 'paid_date',
        'amount_due', 'amount_paid', 'payment_status'
    ]
    bulk_insert(conn, df, 'repayments', cols)


# ── SEQUENCE RESET ─────────────────────────────────────────────────────────────

def reset_sequences(conn):
    """
    After manually inserting IDs, Postgres sequences need to be
    reset — otherwise next SERIAL insert will conflict with existing IDs.
    """
    sequences = [
        ('loan_officer',  'officer_id',   'loan_officer_officer_id_seq'),
        ('applicants',    'applicant_id',  'applicants_applicant_id_seq'),
        ('credit_bureau', 'bureau_id',     'credit_bureau_bureau_id_seq'),
        ('loans',         'loan_id',       'loans_loan_id_seq'),
        ('repayments',    'repayment_id',  'repayments_repayment_id_seq'),
    ]
    with conn.cursor() as cur:
        for table, id_col, seq in sequences:
            cur.execute(f"""
                SELECT setval('{seq}', (SELECT MAX({id_col}) FROM {table}))
            """)
    conn.commit()
    print("  ✓ Sequences reset")


# ── MAIN ───────────────────────────────────────────────────────────────────────

def load_all(officers_df, applicants_df, bureau_df, loans_df, repayments_df):
    print("Connecting to Postgres...")
    conn = get_connection()

    try:
        print("Loading tables...")
        load_officers(conn, officers_df)
        load_applicants(conn, applicants_df)
        load_bureau(conn, bureau_df)
        load_loans(conn, loans_df)
        load_repayments(conn, repayments_df)

        print("Resetting sequences...")
        reset_sequences(conn)

        print("\nLoad complete.")

    except Exception as e:
        conn.rollback()
        print(f"\n✗ Load failed: {e}")
        raise

    finally:
        conn.close()