{{ config(
    materialized='incremental',
    schema='DIMENSIONAL',
    unique_key='payment_sk'
) }}

with final as (
    select * from {{ ref('int_fact_payments') }}
    {% if is_incremental() %}
    where _loaded_at >= (select dateadd(day, -3, max(_loaded_at)) from {{ this }})
    {% endif %}
)

select
    payment_sk,
    customer_id,
    account_id,
    merchant_id,
    payment_date,
    payment_amount,
    fee_amount,
    net_amount,
    is_refund,
    _loaded_at
from final