
    
    

with child as (
    select order_id as from_field
    from DATAEXPERT_STUDENT.WS_PSHARMA.stg_payments
    where order_id is not null
),

parent as (
    select id as to_field
    from DATAEXPERT_STUDENT.WS_PSHARMA.stg_orders
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


