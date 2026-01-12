with first_payroll as (
  select
    account_id,
    min(usage_date) as first_payroll_date
  from {{ ref('fact_product_usage_daily') }}
  where payroll_runs > 0
  group by 1
),

contract_start as (
  select
    account_id,
    min(start_date) as first_contract_start_date
  from {{ ref('fact_contract') }}
  group by 1
)

select
  a.account_id,
  cs.first_contract_start_date,
  fp.first_payroll_date,
  date_diff(fp.first_payroll_date, cs.first_contract_start_date, day) as time_to_first_payroll_days
from {{ ref('dim_account') }} a
left join contract_start cs using(account_id)
left join first_payroll fp using(account_id)
