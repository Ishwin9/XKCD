{{ config(materialized='table') }}

SELECT
    comic_id,
    title,
    title_length,
    alt_text,
    publish_date
FROM {{ ref('std_xkcd_comics') }}