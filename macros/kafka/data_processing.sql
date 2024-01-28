-- macros/process_kafka_data.sqll

{% macro process_kafka_data(source_table, id_column, timestamp_column, output_table) %}

  {% set processed_data = config(
      materialized = 'table',
      unique_key = 'hash( {{ id_column }} )'
  ) %}

  WITH {{ processed_data }} AS (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY {{ id_column }} ORDER BY {{ timestamp_column }} DESC) AS row_num
    FROM {{ ref(source_table) }}
  )

  SELECT *
  FROM {{ processed_data }}
  WHERE row_num = 1

{% endmacro %}
