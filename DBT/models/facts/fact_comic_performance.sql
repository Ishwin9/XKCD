{{ config(materialized='table') }}

SELECT
    dc.comic_sk,
    dd.date_sk,
    RAND() * 10000 AS views,
    base.title_length * 5 AS cost_eur,
    1 + RAND() * 9 AS avg_review_score

FROM {{ ref('stg_xkcd_comics') }}
JOIN {{ ref('dim_comic') }} dc
    ON base.comic_id = dc.comic_id
JOIN {{ ref('dim_date') }} dd
    ON base.publish_date = dd.full_date