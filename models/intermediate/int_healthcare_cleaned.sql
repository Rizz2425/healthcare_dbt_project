{{ config(materialized='table') }}

with staged_data as (
    select * from {{ ref('stg_healthcare_data') }}
)

select
    patient_id,
    
    case 
        when patient_name is null or patient_name = '' then 'NOT PROVIDED'
        else patient_name
    end as patient_name,
    
    case 
        when raw_age < 0 or raw_age > 120 or raw_age is null then 0 
        else raw_age
    end as patient_age,


    case 
        when gender_raw in ('M', 'MALE') then 'Male'
        when gender_raw in ('F', 'FEMALE') then 'Female'
        else 'Unknown'
    end as gender,


    coalesce(
        try_to_date(visit_date, 'YYYY-MM-DD'),
        try_to_date(visit_date, 'MM/DD/YYYY'),
        try_to_date(visit_date, 'YYYY.MM.DD'),
        current_date()  
    ) as visit_date,


    case 
        when upper(billing_amount) = 'FREE' or billing_amount is null then 0
        else abs(try_cast(billing_amount as float))
    end as billing_amount,


    coalesce(diagnosis_code, 'UNKNOWN') as diagnosis_code

from staged_data