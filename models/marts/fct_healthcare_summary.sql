{{ config(materialized='table') }}

with cleaned_data as (
    select * from {{ ref('int_healthcare_cleaned') }}
)

select
    diagnosis_code,
    gender,
    count(patient_id) as total_patients,
    round(sum(billing_amount), 2) as total_revenue,
    round(avg(patient_age), 1) as avg_patient_age,

    current_timestamp() as generated_at
from cleaned_data
where visit_date is not null 
group by 1, 2
order by total_revenue desc