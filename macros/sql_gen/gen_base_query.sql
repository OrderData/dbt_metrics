{% macro gen_base_query(metrics_dictionary, grain, dimensions, secondary_calculations, start_date, end_date, relevant_periods, calendar_dimensions, total_dimension_count, group_name, group_values, where) %}
    {{ return(adapter.dispatch('gen_base_query', 'metrics')(metrics_dictionary, grain, dimensions, secondary_calculations, start_date, end_date, relevant_periods, calendar_dimensions, total_dimension_count, group_name, group_values, where)) }}
{% endmacro %}

{% macro default__gen_base_query(metrics_dictionary, grain, dimensions, secondary_calculations, start_date, end_date, relevant_periods, calendar_dimensions, total_dimension_count, group_name, group_values, where) %}
        {# This is the "base" CTE which selects the fields we need to correctly 
        calculate the metric.  -#}
        select 
            {% if grain -%}
            {#- 
                Given that we've already determined the metrics in metric_names share
                the same windows & filters, we can base the conditional off of the first 
                value in the list because the order doesn't matter. 
            -#}
            cast(base_model.{{group_values.timestamp | replace( '<<date_grain>>' , grain ) }} as {%- if grain in ['hour'] %} timestamp {%- else -%} date {%- endif -%}) as metric_date_{%- if grain in ['hour'] %}minute {%- else -%}day {%- endif -%},
            calendar.date_{{ grain }} as date_{{grain}},
            {%- if grain in ['hour'] %}
            calendar.date_minute as window_filter_date,
            {%- else -%}
            calendar.date_day as window_filter_date,
            {%- endif -%}
                {%- if secondary_calculations | length > 0 %}
                    {%- for period in relevant_periods %}
            calendar.date_{{ period }},
                    {%- endfor -%}
                {%- endif -%}
            {%- endif -%}
            {#- -#}
            {%- for dim in dimensions %}
            base_model.{{ dim }},
            {%- endfor %}
            {%- for calendar_dim in calendar_dimensions -%}
            calendar.{{ calendar_dim }},
            {%- endfor -%}
            {%- for metric_name in group_values.metric_names -%}
            {{ metrics.gen_property_to_aggregate(metrics_dictionary[metric_name], grain, dimensions, calendar_dimensions) }}
            {%- if not loop.last -%},{%- endif -%}
            {%- endfor%}
        from {{ group_values.metric_model }} base_model 
        {# -#}
        {%- if grain or calendar_dimensions|length > 0 -%}
        {{ metrics.gen_calendar_join(group_values, grain) }} 
        {%- endif -%}
        {# #}
        where 1=1
        {#- -#}
        {{ metrics.gen_filters(group_values, start_date, end_date, where, grain) }}
        {# #}

{%- endmacro -%}