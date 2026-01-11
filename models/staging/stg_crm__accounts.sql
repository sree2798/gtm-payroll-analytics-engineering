with source as (
  select * from {{ source('raw', 'crm_accounts') }}
),

renamed as (
  select
    cast(account_id as string) as account_id,
    cast(account_name as string) as account_name,
    cast(employee_count as int64) as employee_count,
    cast(industry as string) as industry,
    cast(region as string) as region,
    cast(acquisition_channel as string) as acquisition_channel,
    cast(created_at as timestamp) as created_at,
    cast(is_customer as int64) as is_customer
  from source
)

select * from renamed
