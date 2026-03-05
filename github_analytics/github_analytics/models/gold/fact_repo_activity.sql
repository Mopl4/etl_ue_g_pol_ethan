{{ config(materialized='table') }}

with
-- -----------------------
-- Sources (Silver)
-- -----------------------
commits as (
  select
    repo_id,
    cast(author_date as date) as activity_date,
    commit_sha,
    author_login
  from {{ ref('stg_commits') }}
  where commit_sha is not null
    and repo_id is not null
    and author_date is not null
),

prs as (
  select
    repo_id,
    cast(created_at as date) as activity_date,
    pr_number,
    is_merged,
    time_to_close_hours
  from {{ ref('stg_pull_requests') }}
  where pr_number is not null
    and repo_id is not null
    and created_at is not null
),

issues as (
  select
    repo_id,
    cast(created_at as date) as activity_date,
    issue_number,
    closed_at,
    time_to_close_hours
  from {{ ref('stg_issues') }}
  where issue_number is not null
    and repo_id is not null
    and created_at is not null
),

-- -----------------------
-- Daily aggregations
-- -----------------------
daily_commits as (
  select
    repo_id,
    activity_date,
    count(*) as commits_count,
    count(distinct author_login) as unique_committers
  from commits
  where author_login is not null
    and lower(author_login) <> 'unknown'
  group by repo_id, activity_date
),

daily_prs as (
  select
    repo_id,
    activity_date,
    count(*) as prs_opened,
    sum(case when is_merged then 1 else 0 end) as prs_merged,
    avg(time_to_close_hours) as avg_pr_close_hours
  from prs
  group by repo_id, activity_date
),

daily_issues as (
  select
    repo_id,
    activity_date,
    count(*) as issues_opened,
    sum(case when closed_at is not null then 1 else 0 end) as issues_closed,
    avg(time_to_close_hours) as avg_issue_close_hours
  from issues
  group by repo_id, activity_date
),

-- -----------------------
-- Date grain union (all repo_id x date that appear anywhere)
-- -----------------------
all_dates as (
  select repo_id, activity_date from daily_commits
  union
  select repo_id, activity_date from daily_prs
  union
  select repo_id, activity_date from daily_issues
),

-- -----------------------
-- Final fact table
-- -----------------------
final as (
  select
    d.repo_id,
    d.activity_date,

    cast(strftime(d.activity_date, '%Y%m%d') as integer) as date_id,

    coalesce(c.commits_count, 0) as commits_count,
    coalesce(c.unique_committers, 0) as unique_committers,

    coalesce(p.prs_opened, 0) as prs_opened,
    coalesce(p.prs_merged, 0) as prs_merged,
    p.avg_pr_close_hours,

    coalesce(i.issues_opened, 0) as issues_opened,
    coalesce(i.issues_closed, 0) as issues_closed,
    i.avg_issue_close_hours

  from all_dates d
  left join daily_commits c
    on c.repo_id = d.repo_id and c.activity_date = d.activity_date
  left join daily_prs p
    on p.repo_id = d.repo_id and p.activity_date = d.activity_date
  left join daily_issues i
    on i.repo_id = d.repo_id and i.activity_date = d.activity_date
)

select *
from final
order by repo_id, activity_date