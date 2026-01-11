with source as (
  select * from {{ source('raw', 'product_usage_daily') }}
),

renamed as (
  select
    cast(usage_date as date) as usage_date,
    cast(account_id as string) as account_id,
    cast(payroll_runs as int64) as payroll_runs,
    cast(active_admins as int64) as active_admins,
    cast(adopt_time as int64) as adopt_time,
    cast(adopt_benefits as int64) as adopt_benefits,
    cast(adopt_full_hcm as int64) as adopt_full_hcm
  from source
)

select * from renamed
