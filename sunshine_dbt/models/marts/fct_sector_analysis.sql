-- models/marts/fct_sector_analysis.sql
{{ config(materialized="table") }}

with sector_data as (select * from {{ ref("stg_salary_canon") }})
select
    sector,
    calendar_year,
    avg(total_compensation) as avg_total_compensation,
    approx_quantiles(total_compensation, 100)[offset(50)] as median_total_compensation,  -- BigQuery syntax
    (
        (
            avg(total_compensation) - lag(avg(total_compensation)) over (
                partition by sector order by calendar_year
            )
        )
    ) / (
        lag(avg(total_compensation)) over (partition by sector order by calendar_year)
    ) as yoy_change,
    min(total_compensation) as min_total_comp,
    max(total_compensation) as max_total_comp,
    count(*) as employee_count
from sector_data
group by sector, calendar_year
having count(*) > 10
order by avg_total_compensation desc
