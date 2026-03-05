with pr_violations as (
  select
    repo_id,
    cast(pr_number as varchar) as record_id
  from {{ ref('stg_pull_requests') }}
  where (closed_at is not null and created_at is not null and closed_at < created_at)
     or (merged_at is not null and created_at is not null and merged_at < created_at)
),

issue_violations as (
  select
    repo_id,
    cast(issue_number as varchar) as record_id
  from {{ ref('stg_issues') }}
  where closed_at is not null
    and created_at is not null
    and closed_at < created_at
)

select
  'pull_request' as violation_type,
  repo_id || '#' || record_id as violation_id
from pr_violations

union all

select
  'issue' as violation_type,
  repo_id || '#' || record_id as violation_id
from issue_violations

