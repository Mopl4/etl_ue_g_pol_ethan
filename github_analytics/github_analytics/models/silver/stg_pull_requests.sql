{{
  config(
    materialized='incremental',
    schema='silver',
    unique_key=['repo_id', 'pr_number'],
    incremental_strategy='delete+insert'
  )
}}

with source as (
  select s.*
  from {{ source('bronze', 'raw_pull_requests') }} s
  {% if is_incremental() %}
  left join {{ this }} e
    on e.repo_id = s.repo_full_name
   and e.pr_number = try_cast(s.pr_number as integer)
  {% endif %}
  where s.pr_number is not null
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
    try_cast(pr_number as integer) as pr_number,

    coalesce(user_login, 'unknown') as user_login,

    try_cast(created_at as timestamp) as created_at,
    try_cast(updated_at as timestamp) as updated_at,
    try_cast(closed_at  as timestamp) as closed_at,
    try_cast(merged_at  as timestamp) as merged_at,

    try_cast(draft as boolean) as is_draft,
    (merged_at is not null) as is_merged,

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
