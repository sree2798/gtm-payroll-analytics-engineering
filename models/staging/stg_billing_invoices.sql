with source as (
  select * from {{ source('raw', 'billing_invoices') }}
),

renamed as (
  select
    cast(invoice_id as string) as invoice_id,
    cast(account_id as string) as account_id,
    cast(contract_id as string) as contract_id,
    cast(invoice_date as date) as invoice_date,
    cast(amount_usd as float64) as amount_usd,
    cast(status as string) as status
  from source
)

select * from renamed
