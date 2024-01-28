-- models/my_sessionized_segment_model.sql

{{ config(
    materialized = 'table',
    unique_key = 'hash( {{ user_id_column }}, {{ timestamp_column }} )'
) }}

{{ sessionize_segment_data('your_segment_source_table', 'timestamp_column', 'user_id_column', 30, 'your_final_table') }}
