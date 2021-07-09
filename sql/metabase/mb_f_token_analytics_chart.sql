{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT token_address
     , transfer_dt
     , n_transfers
     , unique_senders
     , unique_recievers
     , total_value

FROM {{ dynamic_src("models.f_token_analytics_chart") }}
