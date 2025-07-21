with src as (select * from {{ ref('raw_orders') }})

select
    id::int as order_id,
    user_id::int as customer_id,
    order_date::timestamp as order_date,
    status::varchar as order_status
from src
