with
    source as (
        select
            order_id
            , order_date
            , total_amount
            , status
            , last_updated_dt
        from {{ source('bootcamp', 'incremental_example_raw_orders') }}
    )

select *
from source
{% if is_incremental() %}
    where last_updated_dt > (select max(last_updated_dt) from {{ this }})
{% endif %}