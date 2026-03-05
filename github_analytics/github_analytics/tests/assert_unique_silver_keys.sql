with pr_dups as (
  select
    repo_id,
    pr_number,
    count(*) as n
  from {{ ref('stg_pull_requests') }}
  group by repo_id, pr_number
  having count(*) > 1
),

issue_dups as (
  select
    repo_id,
    issue_number,
    count(*) as n
  from {{ ref('stg_issues') }}
  group by repo_id, issue_number
  having count(*) > 1
)

select
  'pull_request_key' as violation_type,
  repo_id || '#' || cast(pr_number as varchar) as violation_id
from pr_dups

union all

select
  'issue_key' as violation_type,
  repo_id || '#' || cast(issue_number as varchar) as violation_id
from issue_dups

