{% macro gen_calendar_join(group_values, grain) %}
    {{ return(adapter.dispatch('gen_calendar_join', 'metrics')(group_values, grain)) }}
{%- endmacro -%}

{% macro default__gen_calendar_join(group_values, grain) %}
        left join calendar
        {%- if group_values.window is not none %}
            on cast(base_model.{{group_values.timestamp | replace( '<<date_grain>>' , grain ) }} as {%- if grain in ['hour'] %}timestamp{%- else -%}date{%- endif -%}) > dateadd({{group_values.window.period}}, -{{group_values.window.count}}, calendar.date_{%- if grain in ['hour'] %}minute{%- else -%}day{%- endif -%})
            and cast(base_model.{{group_values.timestamp | replace( '<<date_grain>>' , grain ) }} as {%- if grain in ['hour'] %}timestamp{%- else -%}date{%- endif -%}) < calendar.date_{%- if grain in ['hour'] %}minute{%- else -%}day{%- endif -%}
        {%- else %}
            on cast(base_model.{{group_values.timestamp | replace( '<<date_grain>>' , grain ) }} as {%- if grain in ['hour'] %}timestamp{%- else -%}date{%- endif -%}) = calendar.date_{%- if grain in ['hour'] %}minute{%- else -%}day{%- endif -%}
        {% endif -%}
{% endmacro %}

{% macro bigquery__gen_calendar_join(group_values, grain) %}
        left join calendar
        {%- if group_values.window is not none %}
            on cast(base_model.{{group_values.timestamp | replace( '<<date_grain>>' , grain ) }} as {%- if grain in ['hour'] %}timestamp{%- else -%}date{%- endif -%}) > date_sub(calendar.date_{%- if grain in ['hour'] %}minute{%- else -%}day{%- endif -%}, interval {{group_values.window.count}} {{group_values.window.period}})
            and cast(base_model.{{group_values.timestamp | replace( '<<date_grain>>' , grain ) }} as {%- if grain in ['hour'] %}timestamp{%- else -%}date{%- endif -%}) < calendar.date_{%- if grain in ['hour'] %}minute{%- else -%}day{%- endif -%}
        {%- else %}
            on cast(base_model.{{group_values.timestamp | replace( '<<date_grain>>' , grain ) }} as {%- if grain in ['hour'] %}timestamp{%- else -%}date{%- endif -%}) = calendar.date_{%- if grain in ['hour'] %}minute{%- else -%}day{%- endif -%}
        {% endif -%}
{% endmacro %}

{% macro postgres__gen_calendar_join(group_values, grain) %}
        left join calendar
        {%- if group_values.window is not none %}
            on cast(base_model.{{group_values.timestamp | replace( '<<date_grain>>' , grain ) }} as {%- if grain in ['hour'] %}timestamp{%- else -%}date{%- endif -%}) > calendar.date_{%- if grain in ['hour'] %}minute{%- else -%}day{%- endif -%} - interval '{{group_values.window.count}} {{group_values.window.period}}'
            and cast(base_model.{{group_values.timestamp | replace( '<<date_grain>>' , grain ) }} as {%- if grain in ['hour'] %}timestamp{%- else -%}date{%- endif -%}) < calendar.date_{%- if grain in ['hour'] %}minute{%- else -%}day{%- endif -%}
        {%- else %}
            on cast(base_model.{{group_values.timestamp | replace( '<<date_grain>>' , grain ) }} as {%- if grain in ['hour'] %}timestamp{%- else -%}date{%- endif -%}) = calendar.date_{%- if grain in ['hour'] %}minute{%- else -%}day{%- endif -%}
        {% endif -%}
{% endmacro %}

{% macro redshift__gen_calendar_join(group_values, grain) %}
        left join calendar
        {%- if group_values.window is not none %}
            on cast(base_model.{{group_values.timestamp | replace( '<<date_grain>>' , grain ) }} as {%- if grain in ['hour'] %}timestamp{%- else -%}date{%- endif -%}) > dateadd({{group_values.window.period}}, -{{group_values.window.count}}, calendar.date_{%- if grain in ['hour'] %}minute{%- else -%}day{%- endif -%})
            and cast(base_model.{{group_values.timestamp | replace( '<<date_grain>>' , grain ) }} as {%- if grain in ['hour'] %}timestamp{%- else -%}date{%- endif -%}) < calendar.date_{%- if grain in ['hour'] %}minute{%- else -%}day{%- endif -%}
        {%- else %}
            on cast(base_model.{{group_values.timestamp | replace( '<<date_grain>>' , grain ) }} as {%- if grain in ['hour'] %}timestamp{%- else -%}date{%- endif -%}) = calendar.date_{%- if grain in ['hour'] %}minute{%- else -%}day{%- endif -%}
        {% endif -%}
{% endmacro %}
