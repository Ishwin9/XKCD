{{ config(materialized='table') }}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['publish_date']) }} AS date_sk,
    publish_date                        AS full_date,
    EXTRACT(YEAR FROM publish_date)     AS year,
    EXTRACT(MONTH FROM publish_date)    AS month,
    EXTRACT(DAY FROM publish_date)      AS day,
    FORMAT_DATE('%A', publish_date)     AS weekday
FROM {{ ref('stg_xkcd_comics') }}