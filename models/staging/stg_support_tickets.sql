with source as (
  select * from {{ source('raw', 'support_tickets') }}
),

renamed as (
  select
    cast(ticket_id as string) as ticket_id,
    cast(account_id as string) as account_id,
    cast(created_at as timestamp) as created_at,
    cast(severity as string) as severity,
    cast(resolution_hours as int64) as resolution_hours,
    cast(csat as int64) as csat
  from source
)

select * from renamed
