{{ config(materialized='table') }}

with ctr as (
  select *
  from {{ ref('fact_contract') }}
),

acct as (
  select
    account_id,
    employee_count,
    employee_band_id,
    employee_band_label,
    industry,
    region,
    acquisition_channel
  from {{ ref('dim_account') }}
)

select
  -- contract fields (keep as-is)
  ctr.*,

  -- account enrichment (prefixed to avoid collisions)
  acct.employee_count            as account_employee_count,
  acct.employee_band_id          as account_employee_band_id,
  acct.employee_band_label       as account_employee_band_label,
  acct.industry                  as account_industry,
  acct.region                    as account_region,
  acct.acquisition_channel       as account_acquisition_channel

from ctr
left join acct
  on ctr.account_id = acct.account_id
