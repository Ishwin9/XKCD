{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['comic_id']) }} AS comic_sk,
    num,
    title,
    title_length,
    alt
FROM {{ ref('stg_xkcd_comics') }}