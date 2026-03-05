select
  r.repo_id
from {{ ref('dim_repository') }} r
left join {{ ref('scoring_repositories') }} s
  on s.repo_id = r.repo_id
where s.repo_id is null

