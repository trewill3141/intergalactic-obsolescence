{% macro parse_json_and_type(source, destination, json_column) %}
    {% set default_types = {
        'string': 'TEXT',
        'integer': 'INTEGER',
        'number': 'FLOAT',
        'boolean': 'BOOLEAN',
        'timestamp': 'TIMESTAMP',
        'date': 'DATE'
    } %}

    {% set json_column_type = source.column_types[json_column] %}
    {% set parsed_json_column = destination ~ '_parsed_json' %}

    -- Extracted JSON column
    WITH parsed_json AS (
        SELECT
            {{ json_column }},
            TRY_CAST({{ json_column }} AS JSON) AS parsed_json
        FROM {{ source }}
    )

    -- Flatten JSON structure
    , flattened_json AS (
        SELECT
            {{ json_column }},
            value::TEXT AS key,
            value::TEXT AS value
        FROM parsed_json,
        LATERAL FLATTEN(input => parsed_json.parsed_json)
    )

    -- Identify and type columns
    , typed_columns AS (
        SELECT
            key,
            CASE
                WHEN IS_ARRAY(value) THEN 'array'
                WHEN IS_OBJECT(value) THEN 'object'
                WHEN TRY_CAST(value AS INTEGER) IS NOT NULL THEN 'integer'
                WHEN TRY_CAST(value AS FLOAT) IS NOT NULL THEN 'number'
                WHEN TRY_CAST(value AS BOOLEAN) IS NOT NULL THEN 'boolean'
                WHEN TRY_CAST(value AS TIMESTAMP) IS NOT NULL THEN 'timestamp'
                WHEN TRY_CAST(value AS DATE) IS NOT NULL THEN 'date'
                ELSE 'string'
            END AS datatype
        FROM flattened_json
    )

    -- Pivot and get final column types
    , final_column_types AS (
        SELECT
            key,
            COALESCE(
                types.datatype,
                {{ default_types['string'] }}
            ) AS datatype
        FROM (
            SELECT DISTINCT key FROM typed_columns
        ) keys
        LEFT JOIN typed_columns types
        ON keys.key = types.key
    )

    -- Create the final parsed table
    , parsed_table AS (
        SELECT
            {{ json_column }},
            {% for key, datatype in final_column_types.items() %}
                TRY_CAST(value:{{ key }} AS {{ datatype }}) AS {{ key }}{% if not loop.last %},{% endif %}
            {% endfor %}
        FROM flattened_json
    )

    -- Insert into destination table
    INSERT INTO {{ destination }} (
        {{ json_column }},
        {% for key, datatype in final_column_types.items() %}
            {{ key }}{% if not loop.last %},{% endif %}
        {% endfor %}
    )
    SELECT
        {{ json_column }},
        {% for key, datatype in final_column_types.items() %}
            {{ key }}{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM parsed_table;

{% endmacro %}
