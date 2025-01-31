with
    source as (
        select
            order_id
            , order_date
            , total_amount
            , status
            , last_updated_dt
        from dataexpert_student.bootcamp.incremental_example_raw_orders
    )

select *
from source
