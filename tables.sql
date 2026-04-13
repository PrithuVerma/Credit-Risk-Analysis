create table loan_officer(
	officer_id serial primary key,
	full_name varchar(100) not null,
	branch varchar(100) not null,
	region varchar(50) not null,
	joined_date date not null
);
create type gender_enum as ENUM('Male', 'Female', 'Other');
create type education_lvl as ENUM('Graduate','Post-Graduate','Other');

create table applicants(
	applicant_id serial primary key,
	full_name varchar(100) not null,
	age int not null check (age between 18 and 75),
	gender gender_enum not null,
	city varchar(100) not null,
	states varchar(100) not null,
	employment_type varchar(50) not null check(employment_type in ('Self', 'Salaried', 'Business Owner')),
	annual_income numeric(12,2) not null,
	education_level education_lvl not null,
	years_employed int not null check(years_employed >= 0),
	marital_status varchar(50) not null check(marital_status in ('Married', 'Divorced', 'Single','Widowed'))
);

create table credit_bureau(
	bureau_id serial primary key,
	applicant_id int not null references applicants(applicant_id),
	credit_score int not null check(credit_score between 300 and 900),
	existing_loans_count int not null default 0,
	total_existing_debt numeric(12,2) not null default 0,
	missed_payments_count int not null default 0,
	bankruptcies int not null default 0,
	bureau_pull_date date not null
);

create type loan_status_enum as ENUM('Approved','Defaulted','Denied','Closed');

create table loans(
	loan_id serial primary key,
	applicant_id int not null references applicants(applicant_id),
	officer_id int not null references loan_officer(officer_id),
	loan_type varchar(100) not null,
	loan_amount numeric(12,2) not null,
	interest_rate numeric(5,2) not null,
	tenure_months int not null,
	application_date date not null,
	approval_date date,
	loan_status loan_status_enum,
	default_flag boolean not null default False
);

create table repayments(
	repayment_id serial primary key,
	loan_id int not null references loans(loan_id),
	due_date date not null,
	paid_date date,
	amount_due numeric(10,2) not null,
	amount_paid numeric(10,2) not null default 0,
	payment_status varchar(20) not null
);

create table ml_scores(
	score_id serial primary key,
	loan_id int not null references loans(loan_id),
	applicant_id int not null references applicants(applicant_id),
	default_probability numeric(5,4) not null,
	risk_tier varchar(20) not null check(risk_tier in ('Low','Medium','High','Very High')),
	model_version varchar(20) not null,
	scored_at timestamp default current_timestamp
)
