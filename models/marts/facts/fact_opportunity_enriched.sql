{{ config(materialized='table') }}

with opp as (
  select *
  from {{ ref('fact_opportunity') }}
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
),

rep as (
  select
    rep_id,
    rep_name
  from {{ ref('dim_sales_rep') }}
)

select
  -- opportunity fields (keep as-is)
  opp.*,

  -- account enrichment (prefixed to avoid column collisions)
  acct.employee_count            as account_employee_count,
  acct.employee_band_id          as account_employee_band_id,
  acct.employee_band_label       as account_employee_band_label,
  acct.industry                  as account_industry,
  acct.region                    as account_region,
  acct.acquisition_channel       as account_acquisition_channel,

  -- rep enrichment
  rep.rep_name                   as rep_name

from opp
left join acct
  on opp.account_id = acct.account_id
left join rep
  on opp.rep_id = rep.rep_id
