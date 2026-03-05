{{ config(materialized='table') }}

with recent_activity as (
  select
    repo_id,
    sum(commits_count) as commits_30d,
    sum(unique_committers) as contributors_30d,
    sum(prs_merged) as merged_prs_30d,

    sum(avg_pr_close_hours * prs_opened) / nullif(sum(prs_opened), 0) as pr_merge_time_hours_30d,
    sum(avg_issue_close_hours * issues_opened) / nullif(sum(issues_opened), 0) as issue_close_time_hours_30d
  from {{ ref('fact_repo_activity') }}
  where activity_date >= current_date - 30
  group by repo_id
),

history_totals as (
  select
    repo_id,
    sum(prs_opened) as total_prs,
    sum(prs_merged) as merged_prs,
    sum(issues_opened) as total_issues,
    sum(issues_closed) as closed_issues
  from {{ ref('fact_repo_activity') }}
  group by repo_id
),

base_metrics as (
  select
    r.repo_id,
    r.repo_name,
    r.owner_login,
    r.language,

    r.stars_count,
    r.forks_count,
    r.watchers_count,

    coalesce(a.commits_30d, 0) as commits_30d,
    coalesce(a.contributors_30d, 0) as contributors_30d,

    a.pr_merge_time_hours_30d,
    a.issue_close_time_hours_30d,

    coalesce(h.total_prs, 0) as total_prs,
    coalesce(h.merged_prs, 0) as merged_prs,
    coalesce(h.total_issues, 0) as total_issues,
    coalesce(h.closed_issues, 0) as closed_issues,

    coalesce(h.merged_prs, 0) * 1.0 / nullif(coalesce(h.total_prs, 0), 0) as merged_pr_ratio,
    coalesce(h.closed_issues, 0) * 1.0 / nullif(coalesce(h.total_issues, 0), 0) as closed_issue_ratio

  from {{ ref('dim_repository') }} r
  left join recent_activity a using (repo_id)
  left join history_totals h using (repo_id)
),

ranked as (
  select
    *,

    ntile(10) over (order by stars_count asc) as rank_stars,
    ntile(10) over (order by forks_count asc) as rank_forks,
    ntile(10) over (order by watchers_count asc) as rank_watchers,

    ntile(10) over (order by commits_30d asc) as rank_commits_30d,
    ntile(10) over (order by contributors_30d asc) as rank_contributors_30d,

    ntile(10) over (order by pr_merge_time_hours_30d desc nulls first) as rank_pr_merge_time,
    ntile(10) over (order by issue_close_time_hours_30d desc nulls first) as rank_issue_close_time,

    ntile(10) over (order by coalesce(merged_pr_ratio, 0) asc) as rank_merged_pr_ratio,
    ntile(10) over (order by coalesce(closed_issue_ratio, 0) asc) as rank_closed_issue_ratio

  from base_metrics
),

scored as (
  select
    *,
    (rank_stars + rank_forks + rank_watchers) * 100.0 / 30 as score_popularity,
    (rank_commits_30d + rank_contributors_30d) * 100.0 / 20 as score_activity,
    (rank_pr_merge_time + rank_issue_close_time) * 100.0 / 20 as score_responsiveness,
    (rank_merged_pr_ratio + rank_closed_issue_ratio) * 100.0 / 20 as score_community
  from ranked
),

final as (
  select
    repo_id,
    repo_name,
    owner_login,
    language,

    stars_count,
    forks_count,
    watchers_count,

    commits_30d,
    contributors_30d,
    pr_merge_time_hours_30d,
    issue_close_time_hours_30d,

    total_prs,
    merged_prs,
    merged_pr_ratio,

    total_issues,
    closed_issues,
    closed_issue_ratio,

    round(score_popularity, 2) as score_popularity,
    round(score_activity, 2) as score_activity,
    round(score_responsiveness, 2) as score_responsiveness,
    round(score_community, 2) as score_community,

    round(
      0.2 * score_popularity
      + 0.3 * score_activity
      + 0.3 * score_responsiveness
      + 0.2 * score_community,
      2
    ) as score_global

  from scored
)

select
  *,
  rank() over (order by score_global desc) as ranking
from final
order by ranking, repo_id

