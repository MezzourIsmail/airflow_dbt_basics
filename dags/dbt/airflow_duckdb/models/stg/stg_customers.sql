with src as (select * from {{ ref('raw_customers') }})

select
    id::int as customer_id,
    first_name::varchar as first_name,
    last_name::varchar as last_name
from src
