-- macros/sessionize_segment_data.sql

{% macro sessionize_segment_data(source_table, timestamp_column, user_id_column, session_timeout_minutes=30, output_table) %}

  {% set sessionized_data = config(
      materialized = 'table',
      unique_key = 'hash( {{ user_id_column }}, {{ timestamp_column }} )'
  ) %}

  WITH events_with_sessions AS (
    SELECT *,
        LAG({ { timestamp_column } }) OVER (
            PARTITION BY { { user_id_column } }
            ORDER BY { { timestamp_column } }
        ) as prev_timestamp,
        CASE
            WHEN TIMESTAMP_DIFF(
                { { timestamp_column } },
                LAG({ { timestamp_column } }) OVER (
                    PARTITION BY { { user_id_column } }
                    ORDER BY { { timestamp_column } }
                ),
                SECOND
            ) > ({ { session_timeout_minutes } } * 60) THEN 1
            ELSE 0
        END as new_session_flag
    FROM { { ref(source_table) } }
),
sessions AS (
    SELECT *,
        SUM(new_session_flag) OVER (
            PARTITION BY { { user_id_column } }
            ORDER BY { { timestamp_column } }
        ) as session_id
    FROM events_with_sessions
)
SELECT *
FROM { { sessions } }
WHERE session_id IS NOT NULL

{% endmacro %}
