{{
  config(
    materialized='incremental',
    schema='silver',
    unique_key=['repo_id', 'issue_number'],
    incremental_strategy='delete+insert'
  )
}}

with source as (
  select s.*
  from {{ source('bronze', 'raw_issues') }} s
  {% if is_incremental() %}
  left join {{ this }} e
    on e.repo_id = s.repo_full_name
   and e.issue_number = try_cast(s.issue_number as integer)
  {% endif %}
  where s.issue_number is not null
  {% if is_incremental() %}
    and (
      e.repo_id is null
      or coalesce(
        try_cast(s.updated_at as timestamp),
        try_cast(s.created_at as timestamp)
      ) > coalesce(e.updated_at, e.created_at)
    )
  {% endif %}
),

cleaned as (
  select
    repo_full_name as repo_id,
    try_cast(issue_number as integer) as issue_number,

    title,
    state,
    user_login,
    try_cast(comments as integer) as comments,
    labels,

    try_cast(created_at as timestamp) as created_at,
    try_cast(updated_at as timestamp) as updated_at,
    try_cast(closed_at  as timestamp) as closed_at,

    try_cast(is_pull_request as boolean) as is_pull_request,

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
