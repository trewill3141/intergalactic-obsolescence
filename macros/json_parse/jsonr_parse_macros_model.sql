-- Your model file

{% set source = source('your_source') %}
{% set destination = ref('your_destination') %}

{{ parse_json_and_type(source=source, destination=destination, json_column='json_column') }}
