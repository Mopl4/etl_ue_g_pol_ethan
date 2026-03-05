{{ config(materialized='view') }}

with source as (
  select * from {{ source('bronze', 'raw_issues') }}
),

cleaned as (
  select
    -- keys
    repo_full_name as repo_id,
    try_cast(issue_number as integer) as issue_number,

    -- useful fields (optionnels mais présents dans ton CSV)
    title,
    state,
    user_login,
    try_cast(comments as integer) as comments,
    labels,

    -- dates
    try_cast(created_at as timestamp) as created_at,
    try_cast(updated_at as timestamp) as updated_at,
    try_cast(closed_at  as timestamp) as closed_at,

    -- cast is_pull_request to BOOLEAN
    try_cast(is_pull_request as boolean) as is_pull_request,

    -- time_to_close_hours (same logic as PRs but only closed_at exists here)
    case
      when closed_at is not null and try_cast(created_at as timestamp) is not null
        then date_diff('hour', try_cast(created_at as timestamp), try_cast(closed_at as timestamp))
      else null
    end as time_to_close_hours

  from source
  where issue_number is not null
)

select *
from cleaned
where coalesce(is_pull_request, false) = false