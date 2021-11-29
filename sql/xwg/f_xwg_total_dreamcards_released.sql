{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT COUNT(DISTINCT "VALUE") AS total_dreamcards_released
  FROM {{ dynamic_src("logs.bsc_token_transfers") }}
 WHERE token_address = '0xe6965b4f189dbdb2bd65e60abaeb531b6fe9580b'
