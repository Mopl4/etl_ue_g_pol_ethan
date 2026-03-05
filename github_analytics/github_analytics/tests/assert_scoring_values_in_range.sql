select
  repo_id,
  score_global,
  ranking
from {{ ref('scoring_repositories') }}
where score_global < 0
   or score_global > 100
   or ranking < 1
   or ranking > 10

