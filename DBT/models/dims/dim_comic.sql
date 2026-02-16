{{ config(materialized='table') }}

SELECT
    comic_id,
    title,
    title_length,
    publish_date
FROM {{ ref('std_xkcd_comics') }}