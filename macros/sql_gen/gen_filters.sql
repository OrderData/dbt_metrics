{%- macro gen_filters(model_values, start_date, end_date, where) -%}
    {{ return(adapter.dispatch('gen_filters', 'metrics')(model_values, start_date, end_date, where)) }}
{%- endmacro -%}

{%- macro default__gen_filters(model_values, start_date, end_date, where) -%}

    {#- metric start/end dates also applied here to limit incoming data -#}
    {% if start_date or end_date %}
        and (
        {% if start_date and end_date -%}
            cast(base_model.{{model_values.timestamp | replace( '<<time_grain>>' , grain ) }} as date) >= cast('{{ start_date }}' as date)
            and cast(base_model.{{model_values.timestamp | replace( '<<time_grain>>' , grain ) }} as date) < cast('{{ end_date }}' as date)
        {%- elif start_date and not end_date -%}
            cast(base_model.{{model_values.timestamp | replace( '<<time_grain>>' , grain ) }} as date) >= cast('{{ start_date }}' as date)
        {%- elif end_date and not start_date -%}
            cast(base_model.{{model_values.timestamp | replace( '<<time_grain>>' , grain ) }} as date) < cast('{{ end_date }}' as date)
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