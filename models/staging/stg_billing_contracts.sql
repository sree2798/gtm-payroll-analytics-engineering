with source as (
  select * from {{ source('raw', 'billing_contracts') }}
),

renamed as (
  select
    cast(contract_id as string) as contract_id,
    cast(account_id as string) as account_id,
    cast(start_date as date) as start_date,
    cast(end_date as date) as end_date,
    cast(product_bundle as string) as product_bundle,
    cast(arr as int64) as arr
  from source
)

select * from renamed
