-- models/my_model.sql

{{ config(
    materialized = 'table',
    unique_key = 'parsed_hash'
) }}

WITH parsed_data AS (
  {{ parse_json('json_column') }}
)

SELECT *
FROM parsed_data