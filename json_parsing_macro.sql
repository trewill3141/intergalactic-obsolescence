-- macros/parse_json.sqll

{% macro parse_json(source_column, output_prefix='parsed') %}

  {% set parsed_data = config(
      materialized = 'view',
      unique_key = 'hash( {{ source_column }} )'
  ) %}

  WITH {{ parsed_data }} AS (
    SELECT
      {{ source_column }},
      PARSE_JSON({{ source_column }})::VARIANT AS json_data
    FROM {{ ref('your_source_table') }}
  )

  SELECT
    {{ source_column }},
    {{ output_prefix }}.*,
    hash( {{ source_column }} ) AS parsed_hash
  FROM (
    SELECT
      {{ source_column }},
      TRY_CAST(json_data:item::VARCHAR, VARCHAR) AS {{ output_prefix }}_{{ replace(item, '.', '_') }}
    FROM {{ parsed_data }},
      LATERAL FLATTEN(input => PARSE_JSON({{ source_column }}))
  )
  PIVOT (MAX({{ output_prefix }}_value) FOR {{ output_prefix }}_key IN ({{ (SELECT DISTINCT replace(item, '.', '_') FROM {{ parsed_data }}) | join(', ') }}))
  AS {{ output_prefix }}

{% endmacro %}