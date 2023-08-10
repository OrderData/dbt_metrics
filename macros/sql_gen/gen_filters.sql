{%- macro gen_filters(model_values, start_date, end_date, where, grain) -%}
    {{ return(adapter.dispatch('gen_filters', 'metrics')(model_values, start_date, end_date, where, grain)) }}
{%- endmacro -%}

{%- macro default__gen_filters(model_values, start_date, end_date, where, grain) -%}

    {%- if grain in ['hour','minute'] %}
        {%- set calendar_date_type = 'timestamp' -%}
    {%- else -%}
        {%- set calendar_date_type = 'date' -%}
    {%- endif %}

    {#- metric start/end dates also applied here to limit incoming data -#}
    {% if start_date or end_date %}
        and (
        {% if start_date and end_date -%}
            {%- if grain in ['hour','minute'] %}
            cast(base_model.{{model_values.timestamp | replace( '<<date_grain>>' , grain ) }} as {{calendar_date_type}}) >= cast('{{ start_date }}' as {{calendar_date_type}})
            and cast(base_model.{{model_values.timestamp | replace( '<<date_grain>>' , grain ) }} as {{calendar_date_type}}) < cast('{{ end_date }}' as {{calendar_date_type}})
            {%- else -%}
            {%- endif %}
        {%- elif start_date and not end_date -%}
            cast(base_model.{{model_values.timestamp | replace( '<<date_grain>>' , grain ) }} as {{calendar_date_type}}) >= cast('{{ start_date }}' as {{calendar_date_type}})
        {%- elif end_date and not start_date -%}
            cast(base_model.{{model_values.timestamp | replace( '<<date_grain>>' , grain ) }} as {{calendar_date_type}}) < cast('{{ end_date }}' as {{calendar_date_type}})
        {%- endif %} 
        )
    {% endif -%} 

    {#- metric filter clauses... -#}
    {% if model_values.filters %}
        and (
            {% for filter in model_values.filters -%}
                {%- if not loop.first -%} and {% endif %}{{ filter.field }} {{ filter.operator }} {{ filter.value }}
            {% endfor -%}
        )
    {% endif -%}

    {% if where != None %}
    and (
        {{ where }}
    )
    {% endif %}

{%- endmacro -%}