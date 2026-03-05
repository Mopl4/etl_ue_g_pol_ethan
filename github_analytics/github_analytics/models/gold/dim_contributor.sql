{{ config(materialized='table') }}

with commit_authors as (
  select
    author_login as login,
    repo_id,
    cast(author_date as timestamp) as activity_at,
    'commit' as activity_type
  from {{ ref('stg_commits') }}
  where author_login is not null
),

pr_authors as (
  select
    user_login as login,
    repo_id,
    cast(created_at as timestamp) as activity_at,
    'pr' as activity_type
  from {{ ref('stg_pull_requests') }}
  where user_login is not null
),

all_activities as (
  select * from commit_authors
  union all
  select * from pr_authors
),

filtered as (
  select *
  from all_activities
  where lower(login) <> 'unknown'
),

agg as (
  select
    login as contributor_id,
    login,

    min(activity_at) as first_contribution_at,

    count(distinct repo_id) as repos_contributed_to,

    count(*) as total_activities

  from filtered
  group by login
)

select *
from agg
order by total_activities desc, repos_contributed_to desc, first_contribution_at asc