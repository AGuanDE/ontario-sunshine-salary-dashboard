-- models/staging/stg_salary_canon.sql
{{
    config(
        materialized='table',
        partition_by={
            "field": "calendar_year",
            "data_type": "int64",
            "range": {
                "start": 1996,
                "end": 2099,
                "interval": 1
            }
        },
        cluster_by=["job_title", "sector"]
    )
}}

with source as (select * from {{ source("staging", "salary-canon") }})

select
    {{
        dbt_utils.generate_surrogate_key(
            [
                "sector",
                "first_name",
                "last_name",
                "employer",
                "job_title",
                "calendar_year",
                "salary_paid",
                "taxable_benefits",
            ]
        )
    }} as hash_id,
    {{ dbt.safe_cast("sector", api.Column.translate_type("STRING")) }} as sector,
    {{ dbt.safe_cast("last_name", api.Column.translate_type("STRING")) }} as last_name,
    {{ dbt.safe_cast("first_name", api.Column.translate_type("STRING")) }}
    as first_name,
    {{ dbt.safe_cast("salary_paid", api.Column.translate_type("NUMERIC")) }}
    as salary_paid,
    {{ dbt.safe_cast("taxable_benefits", api.Column.translate_type("NUMERIC")) }}
    as taxable_benefits,
    {{ dbt.safe_cast("employer", api.Column.translate_type("STRING")) }} as employer,
    {{ dbt.safe_cast("job_title", api.Column.translate_type("STRING")) }} as job_title,
    {{ dbt.safe_cast("calendar_year", api.Column.translate_type("INTEGER")) }}
    as calendar_year,
    {{ dbt.safe_cast("full_name", api.Column.translate_type("STRING")) }} as full_name,
    {{ dbt.safe_cast("total_compensation", api.Column.translate_type("NUMERIC")) }}
    as total_compensation
from source
