
    
    

with all_values as (

    select
        payment_method as value_field,
        count(*) as n_records

    from DATAEXPERT_STUDENT.WS_PSHARMA.stg_payments
    group by payment_method

)

select *
from all_values
where value_field not in (
    'coupon','credit_card','bank_transfer','gift_card'
)


