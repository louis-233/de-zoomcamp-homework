{{ config(materialized='table') }}

SELECT
    *
FROM
    {{ source('ukraine_tweets_all', 'all_tweets_table') }}
    AS atw
LEFT JOIN
    {{ source('ukraine_tweets_all', 'languages_tags_bcp_47') }}
    AS lt
    ON atw.language = lt.tag

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}
  LIMIT 100
{% endif %}

;
