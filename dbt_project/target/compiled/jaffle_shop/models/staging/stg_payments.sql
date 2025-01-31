with
    staging as (
        select
            id
            , order_id
            , payment_method
            , amount
        from dataexpert_student.bootcamp.js_raw_payments
    )

select *
from staging