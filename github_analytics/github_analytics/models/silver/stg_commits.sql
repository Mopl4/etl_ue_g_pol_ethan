{{
  config(
    materialized='incremental',
    schema='silver',
    incremental_strategy='append'
  )
}}

with source as (
  select *
  from {{ source('bronze', 'raw_commits') }}
  {% if is_incremental() %}
  where sha not in (select commit_sha from {{ this }})
  {% endif %}
),

cleaned as (
  select
    sha as commit_sha,
    repo_full_name as repo_id,

    coalesce(author_login, 'unknown') as author_login,

    try_cast(author_date as timestamp) as author_date,
    try_cast(committer_date as timestamp) as committer_date,

    extract('dow'  from try_cast(author_date as timestamp)) as day_of_week,
    extract('hour' from try_cast(author_date as timestamp)) as hour_of_day,

    left(coalesce(message, ''), 200) as message_200

  from source
  where sha is not null
)

select * from cleaned
