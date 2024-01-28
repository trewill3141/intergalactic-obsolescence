-- macros/parse_kafka_json.sqll

{% macro parse_kafka_json(source_column, output_prefix='parsed') %}

  {% set parsed_data = config(
      materialized = 'view',
      unique_key = 'hash( {{ source_column }} )'
  ) %}

  WITH {{ parsed_data }} AS (
    SELECT
      {{ source_column }},
      PARSE_JSON({{ source_column }})::VARIANT AS json_data
    FROM {{ ref('your_kafka_table') }}
  )

  SELECT
    {{ source_column }},
    {{ output_prefix }}.*,
    hash( {{ source_column }} ) AS parsed_hash
  FROM (
    SELECT
      {{ source_column }},
      CASE 
        WHEN TRY_CAST(json_data:item::BOOLEAN, BOOLEAN) IS NOT NULL THEN TRY_CAST(json_data:item::BOOLEAN, BOOLEAN)
        WHEN TRY_CAST(json_data:item::DECIMAL, DECIMAL) IS NOT NULL THEN TRY_CAST(json_data:item::DECIMAL, DECIMAL)
        WHEN TRY_CAST(json_data:item::TIMESTAMP_NTZ, TIMESTAMP_NTZ) IS NOT NULL THEN TRY_CAST(json_data:item::TIMESTAMP_NTZ, TIMESTAMP_NTZ)
        WHEN TRY_CAST(json_data:item::DATE, DATE) IS NOT NULL THEN TRY_CAST(json_data:item::DATE, DATE)
        ELSE TRY_CAST(json_data:item::VARCHAR, VARCHAR)
      END AS {{ output_prefix }}_{{ replace(item, '.', '_') }}
    FROM {{ parsed_data }},
      LATERAL FLATTEN(input => PARSE_JSON({{ source_column }}))
  )
  PIVOT (MAX({{ output_prefix }}_value) FOR {{ output_prefix }}_key IN ({{ (SELECT DISTINCT replace(item, '.', '_') FROM {{ parsed_data }}) | join(', ') }}))
  AS {{ output_prefix }}

{% endmacro %}
