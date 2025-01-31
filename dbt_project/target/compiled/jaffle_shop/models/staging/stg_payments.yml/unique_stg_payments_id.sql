
    
    

select
    id as unique_field,
    count(*) as n_records

from DATAEXPERT_STUDENT.WS_PSHARMA.stg_payments
where id is not null
group by id
having count(*) > 1


