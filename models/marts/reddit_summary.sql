{{ config(materialized='table') }}

SELECT
    subreddit,
    COUNT(*) as post_count,
    AVG(score) as avg_score,
    AVG(num_comments) as avg_comments,
    MAX(score) as max_score
FROM {{ ref('stg_reddit') }}
GROUP BY subreddit