{{
    config(
        materialized='table'
    )
}}

with 
salary_data as (
    select *
    from {{ ref("stg_salary_canon") }}
),
trends as (
    select
        calendar_year,
        avg(total_compensation) as avg_total_compensation,
        count(*) as employee_count
    from salary_data
    group by calendar_year
)
select
    t.*,
    p.dot_com_bubble_burst,
    p.nafta,
    p.2008_global_financial_crisis,
    p.european_debt_crisis,
    p.covid_19_pandemic,
    p.post_covid_inflation_surge,
    p.ontario_bill_124
from trends t
left join {{ ref("dim_econ_events") }} p
    on t.calendar_year = p.calendar_year
order by calendar_year
