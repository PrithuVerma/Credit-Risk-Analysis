-- Row counts
SELECT 'loan_officer' AS tbl, COUNT(*) FROM loan_officer
UNION ALL SELECT 'applicants', COUNT(*) FROM applicants
UNION ALL SELECT 'credit_bureau', COUNT(*) FROM credit_bureau
UNION ALL SELECT 'loans', COUNT(*) FROM loans
UNION ALL SELECT 'repayments', COUNT(*) FROM repayments;

-- Default rate
SELECT
    COUNT(*) AS total_loans,
    SUM(CASE WHEN default_flag THEN 1 ELSE 0 END) AS defaults,
    ROUND(AVG(CASE WHEN default_flag THEN 1.0 ELSE 0 END) * 100, 2) AS default_rate_pct
FROM loans;

-- Sample loans with applicant info
SELECT
    a.full_name,
    a.annual_income,
    l.loan_type,
    l.loan_amount,
    l.loan_status,
    l.default_flag
FROM loans l
JOIN applicants a ON l.applicant_id = a.applicant_id
LIMIT 10;