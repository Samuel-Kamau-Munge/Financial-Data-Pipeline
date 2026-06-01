-- Example dbt model transforming raw data into staging
select
  id,
  Status,
  try_cast(replace(Amount, '$','') as float) as Amount,
  Customer
from {{ ref('raw_accounts_receivable') }}
