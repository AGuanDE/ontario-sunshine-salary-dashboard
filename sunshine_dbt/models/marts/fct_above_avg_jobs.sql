-- models/marts/fct_top_earners.sql
-- Highlights job titles that consistently earn above average
-- Aggregates across years, not just one year at a time

{{ config(materialized="table") }}

with
    job_stats as (
        select
            job_title,
            calendar_year,
            avg(total_compensation) as avg_compensation,
            count(*) as num_people
        from {{ ref("stg_salary_canon") }}
        group by job_title, calendar_year
    ),
    ranked as (
        select
            job_title,
            avg(avg_compensation) as avg_annual_comp,  -- average across years
            count(distinct calendar_year) as years_present,
            sum(num_people) as total_appearances
        from job_stats
        group by job_title
    )
select *
from ranked
where
    avg_annual_comp
    > (select avg(total_compensation) from {{ ref("stg_salary_canon") }})
order by avg_annual_comp desc
