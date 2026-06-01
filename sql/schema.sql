CREATE TABLE IF NOT EXISTS raw_accounts_receivable (
    id serial PRIMARY KEY,
    Status varchar(50),
    Amount varchar(50),
    Customer varchar(255)
);

CREATE TABLE IF NOT EXISTS staging_accounts_receivable (
    id int,
    Status varchar(50),
    Amount numeric,
    Customer varchar(255)
);
