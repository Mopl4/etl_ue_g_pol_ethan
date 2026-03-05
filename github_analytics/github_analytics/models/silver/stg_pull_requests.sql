{{ config(materialized='view') }}

with source as (
  select * from {{ source('bronze', 'raw_pull_requests') }}
),

cleaned as (
  select
    -- keys
    repo_full_name as repo_id,
    try_cast(pr_number as integer) as pr_number,

    -- contributor
    coalesce(user_login, 'unknown') as user_login,

    -- dates
    try_cast(created_at as timestamp) as created_at,
    try_cast(updated_at as timestamp) as updated_at,
    try_cast(closed_at  as timestamp) as closed_at,
    try_cast(merged_at  as timestamp) as merged_at,

    -- booleans
    try_cast(draft as boolean) as is_draft,
    (merged_at is not null) as is_merged,

    -- time_to_close_hours
    case
      when merged_at is not null and try_cast(created_at as timestamp) is not null
        then date_diff('hour', try_cast(created_at as timestamp), try_cast(merged_at as timestamp))
      when closed_at is not null and try_cast(created_at as timestamp) is not null
        then date_diff('hour', try_cast(created_at as timestamp), try_cast(closed_at as timestamp))
      else null
    end as time_to_close_hours

  from source
  where pr_number is not null
)

select *
from cleaned
where pr_number is not null