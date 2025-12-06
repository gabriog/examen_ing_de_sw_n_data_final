
with base as (
    select * from {{ ref('stg_transactions') }}
)

-- Aggregate metrics by customer_id
select
    customer_id,
    count(*) as transaction_count,
    sum(case when status = 'completed' then amount else 0 end) as total_amount_completed,
    sum(amount) as total_amount_all
from base
group by 1
