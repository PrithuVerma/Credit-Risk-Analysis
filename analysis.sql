-- Default Rate by Loan Type and Income Band
SELECT
    l.loan_type,
    CASE
        WHEN a.annual_income < 300000  THEN '< 3L'
        WHEN a.annual_income < 600000  THEN '3L-6L'
        WHEN a.annual_income < 1000000 THEN '6L-10L'
        WHEN a.annual_income < 2000000 THEN '10L-20L'
        ELSE '> 20L'
    END                                                    AS income_band,
    COUNT(*)                                               AS total_loans,
    SUM(CASE WHEN l.default_flag THEN 1 ELSE 0 END)        AS total_defaults,
    ROUND(
        AVG(CASE WHEN l.default_flag THEN 1.0 ELSE 0 END) * 100, 2
    )                                                      AS default_rate_pct,
    ROUND(AVG(l.loan_amount), 2)                           AS avg_loan_amount
FROM loans l
JOIN applicants a ON l.applicant_id = a.applicant_id
GROUP BY l.loan_type, income_band
ORDER BY default_rate_pct DESC;

-- Credit Score Band Analysis with Window Functions
WITH credit_bands AS (
    SELECT
        CASE
            WHEN cb.credit_score < 500 THEN '1. Very Poor (< 500)'
            WHEN cb.credit_score < 600 THEN '2. Poor (500-599)'
            WHEN cb.credit_score < 700 THEN '3. Fair (600-699)'
            WHEN cb.credit_score < 750 THEN '4. Good (700-749)'
            ELSE                            '5. Excellent (750+)'
        END                                                     AS credit_band,
        COUNT(*)                                                AS total_loans,
        SUM(CASE WHEN l.default_flag THEN 1 ELSE 0 END)         AS defaults,
        ROUND(
            AVG(CASE WHEN l.default_flag THEN 1.0 ELSE 0 END) * 100, 2
        )                                                       AS default_rate_pct,
        ROUND(AVG(l.interest_rate), 2)                          AS avg_interest_rate
    FROM loans l
    JOIN credit_bureau cb ON l.applicant_id = cb.applicant_id
    GROUP BY credit_band
)
SELECT
    credit_band,
    total_loans,
    defaults,
    default_rate_pct,
    avg_interest_rate,
    SUM(defaults) OVER (
        ORDER BY credit_band
    )                                                           AS running_total_defaults,
    ROUND(
        SUM(defaults) OVER (ORDER BY credit_band) * 100.0
        / SUM(defaults) OVER (), 2
    )                                                           AS cumulative_default_pct
FROM credit_bands
ORDER BY credit_band;

--Loan Officer Performance Analysis
WITH officer_stats AS (
    SELECT
        lo.officer_id,
        lo.full_name                                            AS officer_name,
        lo.region,
        COUNT(*)                                                AS total_approved,
        SUM(CASE WHEN l.default_flag THEN 1 ELSE 0 END)         AS defaults,
        ROUND(
            AVG(CASE WHEN l.default_flag THEN 1.0 ELSE 0 END) * 100, 2
        )                                                       AS default_rate_pct,
        ROUND(AVG(l.loan_amount), 2)                            AS avg_loan_size,
        ROUND(AVG(l.interest_rate), 2)                          AS avg_interest_rate
    FROM loans l
    JOIN loan_officer lo ON l.officer_id = lo.officer_id
    WHERE l.loan_status != 'Denied'
    GROUP BY lo.officer_id, lo.full_name, lo.region
)
SELECT
    officer_name,
    region,
    total_approved,
    defaults,
    default_rate_pct,
    avg_loan_size,
    avg_interest_rate,
    RANK() OVER (
        PARTITION BY region
        ORDER BY default_rate_pct DESC
    )                                                           AS risk_rank_in_region
FROM officer_stats
ORDER BY region, risk_rank_in_region;

-- Repayment Behavior of Defaulted vs Non-Defaulted Loans
WITH repayment_summary AS (
    SELECT
        l.loan_id,
        l.default_flag,
        l.loan_type,
        COUNT(r.repayment_id)                                   AS total_payments,
        SUM(CASE WHEN r.payment_status = 'Paid'   THEN 1 ELSE 0 END) AS on_time,
        SUM(CASE WHEN r.payment_status = 'Late'   THEN 1 ELSE 0 END) AS late,
        SUM(CASE WHEN r.payment_status = 'Missed' THEN 1 ELSE 0 END) AS missed,
        ROUND(
            SUM(CASE WHEN r.payment_status = 'Missed' THEN 1 ELSE 0 END)
            * 100.0 / NULLIF(COUNT(r.repayment_id), 0), 2
        )                                                       AS missed_rate_pct
    FROM loans l
    JOIN repayments r ON l.loan_id = r.loan_id
    GROUP BY l.loan_id, l.default_flag, l.loan_type
)
SELECT
    loan_type,
    default_flag,
    COUNT(*)                                                    AS loan_count,
    ROUND(AVG(total_payments), 1)                               AS avg_payments_made,
    ROUND(AVG(on_time), 1)                                      AS avg_on_time,
    ROUND(AVG(late), 1)                                         AS avg_late,
    ROUND(AVG(missed), 1)                                       AS avg_missed,
    ROUND(AVG(missed_rate_pct), 2)                              AS avg_missed_rate_pct
FROM repayment_summary
GROUP BY loan_type, default_flag
ORDER BY loan_type, default_flag DESC;

--Rolling 3-Month Default Trend
WITH monthly_defaults AS (
    SELECT
        DATE_TRUNC('month', application_date)                   AS loan_month,
        COUNT(*)                                                AS total_loans,
        SUM(CASE WHEN default_flag THEN 1 ELSE 0 END)           AS defaults,
        ROUND(
            AVG(CASE WHEN default_flag THEN 1.0 ELSE 0 END) * 100, 2
        )                                                       AS default_rate_pct
    FROM loans
    GROUP BY loan_month
)
SELECT
    loan_month,
    total_loans,
    defaults,
    default_rate_pct,
    ROUND(
        AVG(default_rate_pct) OVER (
            ORDER BY loan_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2
    )                                                           AS rolling_3m_default_rate
FROM monthly_defaults
ORDER BY loan_month;

--High Risk Applicant Segmentation Using CTEs
WITH applicant_risk AS (
    SELECT
        a.applicant_id,
        a.full_name,
        a.employment_type,
        a.annual_income,
        cb.credit_score,
        cb.missed_payments_count,
        cb.bankruptcies,
        cb.total_existing_debt,
        ROUND(cb.total_existing_debt / NULLIF(a.annual_income, 0), 4) AS dti_ratio
    FROM applicants a
    JOIN credit_bureau cb ON a.applicant_id = cb.applicant_id
),
risk_flags AS (
    SELECT
        ar.*,
        CASE WHEN ar.credit_score       < 600  THEN 1 ELSE 0 END AS flag_low_credit,
        CASE WHEN ar.dti_ratio          > 0.4  THEN 1 ELSE 0 END AS flag_high_dti,
        CASE WHEN ar.missed_payments_count > 2  THEN 1 ELSE 0 END AS flag_missed_payments,
        CASE WHEN ar.bankruptcies        > 0   THEN 1 ELSE 0 END AS flag_bankruptcy,
        CASE WHEN ar.annual_income       < 300000 THEN 1 ELSE 0 END AS flag_low_income
    FROM applicant_risk ar
),
scored AS (
    SELECT
        *,
        (flag_low_credit + flag_high_dti + flag_missed_payments
         + flag_bankruptcy + flag_low_income)                    AS total_risk_flags
    FROM risk_flags
)
SELECT
    applicant_id,
    full_name,
    employment_type,
    annual_income,
    credit_score,
    dti_ratio,
    missed_payments_count,
    bankruptcies,
    total_risk_flags,
    CASE
        WHEN total_risk_flags >= 4 THEN 'Very High'
        WHEN total_risk_flags = 3  THEN 'High'
        WHEN total_risk_flags = 2  THEN 'Medium'
        ELSE                            'Low'
    END                                                         AS risk_tier,
    RANK() OVER (ORDER BY total_risk_flags DESC, credit_score ASC) AS overall_risk_rank
FROM scored
ORDER BY overall_risk_rank
LIMIT 50;