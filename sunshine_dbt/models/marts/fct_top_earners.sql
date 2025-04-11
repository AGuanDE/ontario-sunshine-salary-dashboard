-- models/marts/fct_top_earners.sql
{{ config(materialized="table") }}

with
    base as (
        select
            calendar_year,
            full_name,
            employer,
            job_title,
            total_compensation,
            row_number() over (
                partition by calendar_year order by total_compensation desc
            ) as rn
        from {{ ref("stg_salary_canon") }}
    )
select calendar_year, full_name, employer, job_title, total_compensation
from base
where rn <= 10
order by calendar_year, rn
