{{ config(materialized='view') }}

with source as (
  select * from {{ source('bronze', 'raw_commits') }}
),

cleaned as (
  select
    -- Rename keys
    sha as commit_sha,
    repo_full_name as repo_id,

    -- Handle NULL author_login
    coalesce(author_login, 'unknown') as author_login,

    -- Cast dates to TIMESTAMP (safe)
    try_cast(author_date as timestamp) as author_date,
    try_cast(committer_date as timestamp) as committer_date,

    -- Extract temporal info from author_date
    extract('dow'  from try_cast(author_date as timestamp)) as day_of_week,
    extract('hour' from try_cast(author_date as timestamp)) as hour_of_day,

    -- Truncate message
    left(coalesce(message, ''), 200) as message_200

  from source
  -- Filter rows where sha IS NULL
  where sha is not null
)

select * from cleaned