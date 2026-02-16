{{ config(materialized='table') }}

SELECT
    dc.comic_id,
    dd.publish_date,
    RAND() * 10000 AS views,
    stg.title_length * 5 AS cost_eur,
    1 + RAND() * 9 AS avg_review_score

FROM {{ ref('std_xkcd_comics') }} stg
JOIN {{ ref('dim_comic') }} dc
    ON stg.comic_id = dc.comic_id
JOIN {{ ref('dim_date') }} dd
    ON stg.publish_date = dd.full_date