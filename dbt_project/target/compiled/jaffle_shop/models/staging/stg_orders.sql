with
    staging as (
        select
            id
            , user_id
            , order_date
            , status
        from dataexpert_student.bootcamp.js_raw_orders
    )

select *
from staging