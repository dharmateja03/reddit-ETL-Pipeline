{{ config(materialized='view') }}

SELECT
    id,
    title,
    score,
    num_comments,
    author,
    created_utc,
    subreddit,
    selftext,
    selftext_length
FROM {{ source('raw', 'reddit') }}