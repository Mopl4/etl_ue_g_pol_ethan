select
  repo_id,
  total_prs,
  merged_prs,
  merged_pr_ratio
from {{ ref('scoring_repositories') }}
where merged_prs > total_prs
   or merged_pr_ratio < 0
   or merged_pr_ratio > 1

