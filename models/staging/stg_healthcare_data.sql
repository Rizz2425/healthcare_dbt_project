{{ config(materialized='view') }}

with raw_data as (
    select *,
           row_number() over (partition by patient_id order by visit_date desc) as rn
    from {{ source('external_source', 'raw_healthcare_data') }}
)

select
    patient_id,
    trim(patient_name) as patient_name,
    try_cast(age as float) as raw_age,
    upper(gender) as gender_raw,
    visit_date,
    billing_amount,
    diagnosis_code
from raw_data
where rn = 1  