with
    staging as (
        select
            id
            , order_id
            , payment_method
            , amount
        from {{ source('bootcamp', 'js_raw_payments') }}
    )

select *
from staging
