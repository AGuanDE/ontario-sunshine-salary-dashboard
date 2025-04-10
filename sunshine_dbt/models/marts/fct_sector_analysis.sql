-- models/marts/fct_sector_analysis.sql

{{
    config(
        materialized='table'
    )
}}

with sector_data as (
    select *
    from {{ ref('stg_salary_canon') }}
)
select
    sector,
    approx_quantiles(total_compensation, 100)[offset(50)] as median_total_compensation, -- BigQuery syntax
    min(total_compensation) as min_total_comp,
    max(total_compensation) as max_total_comp,
    count(*) as employee_count
from sector_data
group by sector
having count(*) > 10
order by median_total_compensation desc