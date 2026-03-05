{{ config(materialized='view') }}

with source as (
  select * from {{ source('bronze', 'raw_repositories') }}
),

cleaned as (
  select
    full_name as repo_id,
    full_name,
    name,
    owner_login,
    split_part(full_name, '/', 2) as repo_name,

    coalesce(description, 'No description') as description,
    coalesce(language, 'Unknown') as language,
    coalesce(license_name, 'Unknown') as license_name,
    topics,

    try_cast(created_at as timestamp) as created_at,
    try_cast(updated_at as timestamp) as updated_at,
    try_cast(pushed_at  as timestamp) as pushed_at,
    try_cast(snapshot_date as date) as snapshot_date,

    try_cast(stargazers_count as integer) as stargazers_count,
    try_cast(watchers_count   as integer) as watchers_count,
    try_cast(forks_count      as integer) as forks_count,
    try_cast(open_issues_count as integer) as open_issues_count,
    try_cast(size             as integer) as size,
    try_cast(network_count    as integer) as network_count,
    try_cast(subscribers_count as integer) as subscribers_count,

    default_branch,
    try_cast(has_wiki  as boolean) as has_wiki,
    try_cast(has_pages as boolean) as has_pages,

    try_cast(archived as boolean) as archived,
    try_cast(disabled as boolean) as disabled,

    date_diff(
      'day',
      cast(try_cast(created_at as timestamp) as date),
      current_date
    ) as repo_age_days

  from source
)

select *
from cleaned
where coalesce(archived, false) = false
