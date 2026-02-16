{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['comic_id']) }} AS comic_sk,
    cosmic_id,
    title,
    title_length,
    alt_text,
    publish_date
FROM {{ ref('std_xkcd_comics') }}