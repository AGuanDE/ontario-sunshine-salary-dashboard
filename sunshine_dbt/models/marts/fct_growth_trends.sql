-- models/marts/fct_growth_trends.sql
{{ config(materialized="table") }}

with
    base as (
        select
            calendar_year,
            sector,
            job_title,
            total_compensation
        from {{ ref("stg_salary_canon") }}
    ),


    agg as (
        select
            calendar_year,
            sector,
            job_title,
            count(*) as employee_count,
            avg(total_compensation) as avg_total_compensation,
            sum(total_compensation) as total_compensation_sum,
            min(total_compensation) as min_total_compensation,
            max(total_compensation) as max_total_compensation
        from base
        group by calendar_year, sector, job_title
    ),

    -- Step 3: Compute growth trends using window functions.
    metrics as (
        select
            calendar_year,
            sector,
            job_title,
            employee_count,
            avg_total_compensation,
            total_compensation_sum,
            min_total_compensation,
            max_total_compensation,
            lag(employee_count) over (
                partition by sector, job_title order by calendar_year
            ) as prev_employee_count,
            lag(avg_total_compensation) over (
                partition by sector, job_title order by calendar_year
            ) as prev_avg_total_compensation,
            lag(total_compensation_sum) over (
                partition by sector, job_title order by calendar_year
            ) as prev_total_compensation_sum
        from agg
    )

-- Step 4: Calculate growth rates; percentage change year-over-year.
select
    calendar_year,
    sector,
    job_title,
    employee_count,
    avg_total_compensation,
    total_compensation_sum,
    -- Employee count growth rate: (Current - Previous) / Previous
    case
        when prev_employee_count is null or prev_employee_count = 0
        then null
        else (employee_count - prev_employee_count) / prev_employee_count
    end as employee_count_growth_rate,
    -- Average compensation growth rate: (Current - Previous) / Previous
    case
        when prev_avg_total_compensation is null or prev_avg_total_compensation = 0
        then null
        else
            (avg_total_compensation - prev_avg_total_compensation)
            / prev_avg_total_compensation
    end as avg_compensation_growth_rate,
    -- Total compensation growth rate: (Current - Previous) / Previous
    case
        when prev_total_compensation_sum is null or prev_total_compensation_sum = 0
        then null
        else
            (total_compensation_sum - prev_total_compensation_sum)
            / prev_total_compensation_sum
    end as total_compensation_growth_rate,
    min_total_compensation,
    max_total_compensation
from metrics
order by calendar_year, sector, job_title
