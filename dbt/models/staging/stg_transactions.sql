{% set clean_dir = var('clean_dir') %}
{% set ds_nodash = var('ds_nodash') %}

with source as (
    select *
    from read_parquet(
        '{{ clean_dir }}/transactions_{{ ds_nodash }}_clean.parquet'
    )
)

-- Select and rename columns to match schema.yml
select
    transaction_id,
    customer_id,
    amount::double as amount,
    status,
    transaction_ts::timestamp as transaction_ts,
    date_trunc('day', transaction_ts::timestamp)::date as transaction_date
from source
