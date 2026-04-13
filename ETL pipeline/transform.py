import pandas as pd
import numpy as nps

# ── CLEANING ───────────────────────────────────────────────────────────────────

def clean_officers(df):
    df = df.copy()
    df['full_name'] = df['full_name'].str.strip().str.title()
    df['branch']    = df['branch'].str.strip().str.title()
    df['region']    = df['region'].str.strip().str.title()
    return df


def clean_applicants(df):
    df = df.copy()
    df['full_name'] = df['full_name'].str.strip().str.title()
    df['city']      = df['city'].str.strip().str.title()
    df['states']    = df['states'].str.strip().str.title()

    # Clamp income — floor at 150k, cap at 10M (removes extreme Faker outliers)
    df['annual_income'] = df['annual_income'].clip(lower=150000, upper=10000000)
    df['annual_income'] = df['annual_income'].round(2)

    df['age'] = df['age'].clip(lower=18, upper=75)

    return df


def clean_bureau(df):
    df = df.copy()
    df['total_existing_debt']  = df['total_existing_debt'].clip(lower=0).round(2)
    df['credit_score']         = df['credit_score'].clip(lower=300, upper=900)
    df['missed_payments_count']= df['missed_payments_count'].clip(lower=0)
    df['bankruptcies']         = df['bankruptcies'].clip(lower=0)
    df['existing_loans_count'] = df['existing_loans_count'].clip(lower=0)
    return df


def clean_loans(df):
    df = df.copy()
    df['loan_amount']    = df['loan_amount'].clip(lower=10000).round(2)
    df['interest_rate']  = df['interest_rate'].clip(lower=5.0, upper=30.0).round(2)
    df['loan_status']    = df['loan_status'].str.strip()

    # Denied loans should never have approval_date
    df.loc[df['loan_status'] == 'Denied', 'approval_date'] = None
    return df


def clean_repayments(df):
    df = df.copy()
    df['amount_due']  = df['amount_due'].clip(lower=0).round(2)
    df['amount_paid'] = df['amount_paid'].clip(lower=0).round(2)

    # Missed payments should have amount_paid = 0
    df.loc[df['payment_status'] == 'Missed', 'amount_paid'] = 0
    df.loc[df['payment_status'] == 'Missed', 'paid_date']   = None
    return df


# ── FEATURE ENGINEERING ────────────────────────────────────────────────────────

def engineer_features(applicants_df, bureau_df, loans_df):
    """
    Creates analytical columns used by both SQL analysis and ML model.
    These are stored in the transformed DataFrames — not separate tables.
    The ML notebook will pull these via SQL queries later.
    """

    # Merge applicants + bureau for ratio calculations
    merged = applicants_df.merge(bureau_df, on='applicant_id')

    # 1. Debt-to-Income Ratio (DTI)
    # What % of annual income is already owed as debt
    # Rule of thumb: DTI > 40% is high risk
    merged['dti_ratio'] = (
        merged['total_existing_debt'] / merged['annual_income']
    ).round(4)

    # 2. Credit Score Band
    # Bucketing raw score into analyst-friendly tiers
    merged['credit_band'] = pd.cut(
        merged['credit_score'],
        bins    = [299, 499, 599, 699, 749, 900],
        labels  = ['Very Poor', 'Poor', 'Fair', 'Good', 'Excellent']
    ).astype(str)

    # 3. Income Band
    merged['income_band'] = pd.cut(
        merged['annual_income'],
        bins   = [0, 300000, 600000, 1000000, 2000000, float('inf')],
        labels = ['< 3L', '3L-6L', '6L-10L', '10L-20L', '> 20L']
    ).astype(str)

    loans_merged = loans_df.merge(
        merged[['applicant_id', 'annual_income', 'dti_ratio',
                'credit_score', 'credit_band', 'income_band',
                'missed_payments_count', 'bankruptcies']],
        on='applicant_id'
    )

    # 4. Loan-to-Income Ratio (LTI)
    # How large is this loan relative to annual income
    loans_merged['lti_ratio'] = (
        loans_merged['loan_amount'] / loans_merged['annual_income']
    ).round(4)

    # 5. EMI Estimate
    # Monthly Equated Installment — standard banking metric
    # Formula: EMI = P * r * (1+r)^n / ((1+r)^n - 1)
    r = loans_merged['interest_rate'] / 100 / 12  # monthly rate
    n = loans_merged['tenure_months']
    P = loans_merged['loan_amount']

    loans_merged['estimated_emi'] = (
        P * r * (1 + r)**n / ((1 + r)**n - 1)
    ).round(2)

    # 6. EMI-to-Income Ratio
    # Monthly EMI as % of monthly income — key affordability metric
    monthly_income = loans_merged['annual_income'] / 12
    loans_merged['emi_to_income_ratio'] = (
        loans_merged['estimated_emi'] / monthly_income
    ).round(4)

    # 7. Risk Score (rule-based, pre-ML)
    # Simple weighted score for SQL analysis — not the ML model output
    loans_merged['rule_based_risk_score'] = (
          (900 - loans_merged['credit_score']) * 0.4
        + loans_merged['dti_ratio'] * 100 * 0.3
        + loans_merged['lti_ratio'] * 100 * 0.2
        + loans_merged['missed_payments_count'] * 10 * 0.1
    ).round(2)

    return merged, loans_merged


# ── VALIDATION ─────────────────────────────────────────────────────────────────

def validate(officers_df, applicants_df, bureau_df, loans_df, repayments_df):
    issues = []

    if applicants_df['annual_income'].lt(0).any():
        issues.append("Negative income values found in applicants")

    if bureau_df['credit_score'].lt(300).any() or bureau_df['credit_score'].gt(900).any():
        issues.append("Credit scores out of range [300-900]")

    if loans_df['loan_amount'].lt(0).any():
        issues.append("Negative loan amounts found")

    if loans_df['default_flag'].isna().any():
        issues.append("Null values in default_flag")

    denied_with_approval = loans_df[
        (loans_df['loan_status'] == 'Denied') &
        (loans_df['approval_date'].notna())
    ]
    if len(denied_with_approval) > 0:
        issues.append(f"{len(denied_with_approval)} denied loans have approval dates")

    if issues:
        print("── Validation Issues ──")
        for i in issues:
            print(f"  ✗ {i}")
    else:
        print("── Validation passed — all checks clean ──")

    return len(issues) == 0


# ── MAIN ───────────────────────────────────────────────────────────────────────

def transform_all(officers_df, applicants_df, bureau_df, loans_df, repayments_df):
    print("Cleaning data...")
    officers_df    = clean_officers(officers_df)
    applicants_df  = clean_applicants(applicants_df)
    bureau_df      = clean_bureau(bureau_df)
    loans_df       = clean_loans(loans_df)
    repayments_df  = clean_repayments(repayments_df)

    print("Engineering features...")
    applicants_enriched, loans_enriched = engineer_features(
        applicants_df, bureau_df, loans_df
    )

    print("Validating...")
    valid = validate(officers_df, applicants_df, bureau_df,loans_df, repayments_df)

    if not valid:
        raise ValueError("Data validation failed — fix issues before loading")

    print("Transform complete.\n")

    return (
        officers_df,
        applicants_df,
        bureau_df,
        loans_df,
        repayments_df,
        applicants_enriched,
        loans_enriched
    )