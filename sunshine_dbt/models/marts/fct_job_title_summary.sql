-- models/marts/fct_job_title_summary.sql

{{
    config(
        materialized='table'
    )
}}

with job_data as (
    select *
    from {{ ref('stg_salary_canon') }}
)
select
    job_title,
    calendar_year,
    count(*) as num_employees,
    avg(total_compensation) as avg_total_compensation
from job_data
group by job_title, calendar_year
order by job_title, calendar_year desc