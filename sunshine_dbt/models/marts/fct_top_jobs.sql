-- models/marts/fct_top_jobs.sql
{{ config(materialized="table") }}

with
    job_aggregates as (
        select
            calendar_year,
            job_title,
            avg(total_compensation) as avg_total_compensation,
            approx_quantiles(total_compensation, 100)[
                offset(50)
            ] as median_total_compensation,
            count(*) as employee_count
        from {{ ref("stg_salary_canon") }}
        group by calendar_year, job_title
    ),

    ranked as (
        select
            calendar_year,
            job_title,
            avg_total_compensation,
            median_total_compensation,
            employee_count,
            -- Rank job titles in each calendar_year by average total compensation
            -- (highest first)
            row_number() over (
                partition by calendar_year order by avg_total_compensation desc
            ) as avg_rank,
            -- Rank job titles in each calendar_year by median total compensation
            -- (highest first)
            row_number() over (
                partition by calendar_year order by median_total_compensation desc
            ) as median_rank
        from job_aggregates
    )

select
    calendar_year,
    job_title,
    avg_total_compensation,
    median_total_compensation,
    employee_count,
    avg_rank,
    median_rank
from ranked
where employee_count > 10
order by calendar_year, avg_rank desc
