import pandas as pd
import numpy as np
from faker import Faker
from faker.providers import person, address
import random
from datetime import date, timedelta
import config

fake = Faker('en_IN')  # Indian locale for realistic names/cities
np.random.seed(42)
random.seed(42)

# ── CONSTANTS ──────────────────────────────────────────────────────────────────

N_OFFICERS   = 30
N_APPLICANTS = 2000
N_LOANS      = 2500   # some applicants have multiple loans

LOAN_TYPES     = ['Personal', 'Home', 'Auto', 'Education']
REGIONS        = ['North', 'South', 'East', 'West', 'Central']
EMPLOYMENT     = ['Salaried', 'Self', 'Business Owner']
EDUCATION      = ['Graduate', 'Post-Graduate', 'Other']
GENDER         = ['Male', 'Female', 'Other']
MARITAL_STATUS = ['Married', 'Single', 'Divorced', 'Widowed']

INDIAN_CITIES_STATES = [
    ('Mumbai', 'Maharashtra'), ('Delhi', 'Delhi'), ('Bangalore', 'Karnataka'),
    ('Hyderabad', 'Telangana'), ('Chennai', 'Tamil Nadu'), ('Kolkata', 'West Bengal'),
    ('Pune', 'Maharashtra'), ('Ahmedabad', 'Gujarat'), ('Jaipur', 'Rajasthan'),
    ('Lucknow', 'Uttar Pradesh'), ('Surat', 'Gujarat'), ('Chandigarh', 'Punjab'),
    ('Bhopal', 'Madhya Pradesh'), ('Indore', 'Madhya Pradesh'), ('Patna', 'Bihar')
]

# ── HELPER FUNCTIONS ───────────────────────────────────────────────────────────

def random_date(start_year=2018, end_year=2023):
    start = date(start_year, 1, 1)
    end   = date(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def generate_income(employment_type):
    """Income distribution varies by employment type — reflects reality"""
    if employment_type == 'Salaried':
        return round(np.random.normal(loc=700000, scale=200000), 2)
    elif employment_type == 'Business Owner':
        return round(np.random.normal(loc=1200000, scale=400000), 2)
    else:  # Self
        return round(np.random.normal(loc=500000, scale=150000), 2)

def generate_credit_score(income, missed_payments, bankruptcies):
    """
    Credit score correlated with income and payment history.
    This gives the ML model a real signal to learn from.
    """
    base = np.random.normal(loc=650, scale=80)
    base += (income / 100000) * 5       # higher income → slightly better score
    base -= missed_payments * 20        # each missed payment hurts
    base -= bankruptcies * 80           # bankruptcy tanks the score
    return int(np.clip(base, 300, 900))

def generate_interest_rate(credit_score, loan_type):
    """Lower credit score → higher interest rate. Basic risk pricing."""
    base_rates = {
        'Personal':  12.0,
        'Home':       8.5,
        'Auto':       9.5,
        'Education': 10.0
    }
    base = base_rates[loan_type]
    if credit_score < 550:
        base += random.uniform(4.0, 6.0)
    elif credit_score < 650:
        base += random.uniform(2.0, 4.0)
    elif credit_score < 750:
        base += random.uniform(0.5, 2.0)
    return round(base, 2)

def generate_default_flag(credit_score, income, existing_debt, missed_payments,
                        loan_amount, tenure_months):
    """
    Core function — assigns default probability based on risk factors.
    This is what the ML model will try to learn.
    ~18% overall default rate.
    """
    # Debt-to-income ratio
    dti = existing_debt / max(income, 1)

    prob = 0.05  # base probability

    # Credit score bands
    if credit_score < 500:
        prob += 0.35
    elif credit_score < 600:
        prob += 0.20
    elif credit_score < 700:
        prob += 0.08

    # Missed payment history
    prob += missed_payments * 0.06

    # High DTI
    if dti > 0.5:
        prob += 0.12
    elif dti > 0.3:
        prob += 0.05

    # Large loan relative to income
    if loan_amount > income * 0.8:
        prob += 0.08

    # Longer tenure = higher exposure
    if tenure_months >= 120:
        prob += 0.05
    elif tenure_months >= 60:
        prob += 0.02

    prob = np.clip(prob, 0.01, 0.95)
    return bool(np.random.binomial(1, prob))

# ── GENERATOR FUNCTIONS ────────────────────────────────────────────────────────

def generate_officers():
    records = []
    for _ in range(N_OFFICERS):
        records.append({
            'full_name'  : fake.name(),
            'branch'     : fake.city(),
            'region'     : random.choice(REGIONS),
            'joined_date': random_date(2010, 2020)
        })
    return pd.DataFrame(records)


def generate_applicants():
    records = []
    for _ in range(N_APPLICANTS):
        emp   = random.choice(EMPLOYMENT)
        city, state = random.choice(INDIAN_CITIES_STATES)
        income = max(generate_income(emp), 150000)  # floor at 1.5L

        records.append({
            'full_name'       : fake.name(),
            'age'             : random.randint(21, 70),
            'gender'          : random.choice(GENDER),
            'city'            : city,
            'states'          : state,
            'employment_type' : emp,
            'annual_income'   : income,
            'education_level' : random.choice(EDUCATION),
            'years_employed'  : random.randint(0, 30),
            'marital_status'  : random.choice(MARITAL_STATUS)
        })
    return pd.DataFrame(records)


def generate_credit_bureau(applicants_df):
    records = []
    for _, row in applicants_df.iterrows():
        missed     = random.choices([0, 1, 2, 3, 4, 5], weights=[50,20,12,8,6,4])[0]
        bankrupts  = random.choices([0, 1, 2], weights=[88, 9, 3])[0]
        ex_loans   = random.randint(0, 5)
        ex_debt    = round(random.uniform(0, row['annual_income'] * 0.6), 2)
        c_score    = generate_credit_score(row['annual_income'], missed, bankrupts)

        records.append({
            'applicant_id'          : row['applicant_id'],
            'credit_score'          : c_score,
            'existing_loans_count'  : ex_loans,
            'total_existing_debt'   : ex_debt,
            'missed_payments_count' : missed,
            'bankruptcies'          : bankrupts,
            'bureau_pull_date'      : random_date(2018, 2023)
        })
    return pd.DataFrame(records)


def generate_loans(applicants_df, officers_df, bureau_df):
    records     = []
    officer_ids = officers_df['officer_id'].tolist()

    # Merge bureau data for default flag calculation
    merged = applicants_df.merge(bureau_df, on='applicant_id')

    for _ in range(N_LOANS):
        row         = merged.sample(1).iloc[0]
        loan_type   = random.choice(LOAN_TYPES)
        app_date    = random_date(2019, 2023)

        # Loan amount scaled to income and loan type
        multipliers = {'Home': 5.0, 'Auto': 1.5, 'Personal': 0.8, 'Education': 1.2}
        loan_amount = round(
            row['annual_income'] * multipliers[loan_type] * random.uniform(0.5, 1.2), 2
        )
        tenure      = random.choice([12, 24, 36, 60, 120])
        rate        = generate_interest_rate(row['credit_score'], loan_type)

        defaulted = generate_default_flag(
            credit_score    = row['credit_score'],
            income          = row['annual_income'],
            existing_debt   = row['total_existing_debt'],
            missed_payments = row['missed_payments_count'],
            loan_amount     = loan_amount,
            tenure_months   = tenure
        )

        if defaulted:
            status       = 'Defaulted'
            approval_date = app_date + timedelta(days=random.randint(3, 15))
        else:
            status        = random.choices(
                ['Approved', 'Denied', 'Closed'],
                weights=[55, 25, 20]
            )[0]
            approval_date = app_date + timedelta(days=random.randint(3, 15)) \
                            if status != 'Denied' else None

        records.append({
            'applicant_id'     : int(row['applicant_id']),
            'officer_id'       : random.choice(officer_ids),
            'loan_type'        : loan_type,
            'loan_amount'      : loan_amount,
            'interest_rate'    : rate,
            'tenure_months'    : tenure,
            'application_date' : app_date,
            'approval_date'    : approval_date,
            'loan_status'      : status,
            'default_flag'     : defaulted
        })

    return pd.DataFrame(records)


def generate_repayments(loans_df):
    records = []

    # Only generate repayments for non-denied loans
    active_loans = loans_df[loans_df['loan_status'] != 'Denied'].copy()

    for _, loan in active_loans.iterrows():
        monthly_due = round(loan['loan_amount'] / loan['tenure_months'], 2)
        n_payments  = min(loan['tenure_months'], random.randint(3, loan['tenure_months']))

        for i in range(n_payments):
            due_date = loan['application_date'] + timedelta(days=30 * (i + 1))

            if loan['default_flag']:
                # Defaulted loans: good payments early, then start missing
                cutoff = int(loan['tenure_months'] * 0.4)
                if i < cutoff:
                    status     = 'Paid'
                    paid_date  = due_date + timedelta(days=random.randint(0, 5))
                    amount_paid = monthly_due
                else:
                    status      = random.choices(['Late', 'Missed'], weights=[30, 70])[0]
                    paid_date   = due_date + timedelta(days=random.randint(10, 40)) \
                                if status == 'Late' else None
                    amount_paid = monthly_due if status == 'Late' else 0
            else:
                status      = random.choices(['Paid', 'Late'], weights=[90, 10])[0]
                paid_date   = due_date + timedelta(days=random.randint(0, 3)) \
                            if status == 'Paid' \
                            else due_date + timedelta(days=random.randint(5, 20))
                amount_paid = monthly_due

            records.append({
                'loan_id'        : int(loan['loan_id']),
                'due_date'       : due_date,
                'paid_date'      : paid_date,
                'amount_due'     : monthly_due,
                'amount_paid'    : amount_paid,
                'payment_status' : status
            })

    return pd.DataFrame(records)


# ── MAIN ───────────────────────────────────────────────────────────────────────

def generate_all():
    print("Generating loan officers...")
    officers_df = generate_officers()
    officers_df['officer_id'] = range(1, len(officers_df) + 1)

    print("Generating applicants...")
    applicants_df = generate_applicants()
    applicants_df['applicant_id'] = range(1, len(applicants_df) + 1)

    print("Generating credit bureau data...")
    bureau_df = generate_credit_bureau(applicants_df)
    bureau_df['bureau_id'] = range(1, len(bureau_df) + 1)

    print("Generating loans...")
    loans_df = generate_loans(applicants_df, officers_df, bureau_df)
    loans_df['loan_id'] = range(1, len(loans_df) + 1)

    print("Generating repayments...")
    repayments_df = generate_repayments(loans_df)
    repayments_df['repayment_id'] = range(1, len(repayments_df) + 1)

    print(f"\n── Dataset Summary ──────────────────────")
    print(f"Officers    : {len(officers_df)}")
    print(f"Applicants  : {len(applicants_df)}")
    print(f"Bureau rows : {len(bureau_df)}")
    print(f"Loans       : {len(loans_df)}")
    print(f"Repayments  : {len(repayments_df)}")
    print(f"Default rate: {loans_df['default_flag'].mean():.1%}")
    print(f"─────────────────────────────────────────\n")

    return officers_df, applicants_df, bureau_df, loans_df, repayments_df


if __name__ == '__main__':
    generate_all()