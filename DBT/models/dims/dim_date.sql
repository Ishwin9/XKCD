{{ config(materialized='table') }}

SELECT DISTINCT
    publish_date                        AS full_date,
    EXTRACT(YEAR FROM publish_date)     AS year,
    EXTRACT(MONTH FROM publish_date)    AS month,
    EXTRACT(DAY FROM publish_date)      AS day,
    FORMAT_DATE('%A', publish_date)     AS weekday
FROM {{ ref('std_xkcd_comics') }}