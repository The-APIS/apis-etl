USE SCHEMA {{ schema }};

COPY INTO {{ user_prefix }}{{ currency }}_{{ table }} FROM @{{ stage }}/{{ table }}/;

REMOVE @{{ stage }}/{{ table }};
