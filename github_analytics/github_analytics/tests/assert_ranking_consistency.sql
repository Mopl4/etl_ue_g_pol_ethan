with s as (
  select
    repo_id,
    score_global,
    ranking
  from {{ ref('scoring_repositories') }}
),

dup_rank as (
  select ranking
  from s
  group by ranking
  having count(*) > 1
),

wrong_top as (
  select repo_id
  from s
  where ranking = 1
    and score_global < (select max(score_global) from s)
)

select
  'duplicate_ranking' as violation_type,
  cast(ranking as varchar) as violation_id
from dup_rank

union all

select
  'wrong_top_repo' as violation_type,
  repo_id as violation_id
from wrong_top

