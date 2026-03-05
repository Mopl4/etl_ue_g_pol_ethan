{{ config(materialized='table') }}

with date_spine as (
  select
    unnest(
      generate_series(
        current_date - interval '5 years',
        current_date,
        interval '1 day'
      )
    )::date as full_date
),

enriched as (
  select
    -- surrogate key YYYYMMDD
    cast(strftime(full_date, '%Y%m%d') as integer) as date_id,

    full_date,

    -- temporal fields
    extract('year' from full_date) as year,
    extract('month' from full_date) as month,
    extract('week' from full_date) as week_of_year,
    extract('dow' from full_date) as day_of_week,      -- 0=Sunday ... 6=Saturday (DuckDB)

    -- readable names
    strftime(full_date, '%A') as day_name,             -- Monday, Tuesday...
    strftime(full_date, '%B') as month_name,           -- January, February...

    -- weekend flag
    (extract('dow' from full_date) in (0, 6)) as is_weekend,

    -- quarter
    extract('quarter' from full_date) as quarter

  from date_spine
)

select * from enriched
order by full_date