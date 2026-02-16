{{ config(materialized='view') }}

SELECT
    CAST(num AS INT64) AS comic_id,
    title AS title,
    LENGTH(title) AS title_length,
    alt AS alt_text,
    DATE(
        CAST(year AS INT64),
        CAST(month AS INT64),
        CAST(day AS INT64)
    ) AS publish_date
FROM {{ source('xkcd', 'comics') }}