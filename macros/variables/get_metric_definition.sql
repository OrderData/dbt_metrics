{% macro get_metric_definition(metric_definition) %}

    {% set metrics_dictionary_dict = {} %}

    {% if metric_definition is mapping %}

        {# THIS IS THE PATH FOR DEVELOP #}

        {% do metrics_dictionary_dict.update({'name': metric_definition.name})%}
        {% do metrics_dictionary_dict.update({'calculation_method': metric_definition.calculation_method})%}
        {# We need to do some cleanup for metric parsing #}
        {% set metric_expression = metric_definition.expression | replace("metric(","") | replace(")","") | replace("{{","") | replace("}}","")  | replace("'","") | replace('"',"")  %}
        {% do metrics_dictionary_dict.update({'expression': metric_expression})%} 
        {% do print(metric_expression)%}
        {% do metrics_dictionary_dict.update({'timestamp': metric_definition.timestamp})%}
        {% do metrics_dictionary_dict.update({'time_grains': metric_definition.time_grains})%}
        {% do metrics_dictionary_dict.update({'dimensions': metric_definition.dimensions})%}
        {% do metrics_dictionary_dict.update({'filters': metric_definition.filters})%}

        {% if metric_definition.calculation_method != 'derived' %}
            {% set metric_model_name = metrics.get_metric_model_name(metric_model=metric_definition.model) %}
            {% do metrics_dictionary_dict.update({'metric_model': metrics.get_model_relation(metric_model_name, metric_name)}) %}
        {% endif %}

        {% if metric_definition.window %}
            {% do metrics_dictionary_dict.update({'window': metric_definition.window}) %}
        {% else %}
            {% do metrics_dictionary_dict.update({'window': None}) %}
        {% endif %}

    {% else %}

        {# THIS IS THE PATH FOR THE METRIC OBJECT #}
        {% do metrics_dictionary_dict.update({'name': metric_definition.name})%}
        {% do metrics_dictionary_dict.update({'calculation_method': metric_definition.calculation_method})%}
        {% do metrics_dictionary_dict.update({'expression': metric_definition.expression})%} 
        {% do metrics_dictionary_dict.update({'timestamp': metric_definition.timestamp})%}
        {% do metrics_dictionary_dict.update({'time_grains': metric_definition.time_grains})%}
        {% do metrics_dictionary_dict.update({'dimensions': metric_definition.dimensions})%}
        {% do metrics_dictionary_dict.update({'filters': metric_definition.filters})%}
        {% do metrics_dictionary_dict.update({'window': metric_definition.window})%}

        {% if metric_definition.calculation_method != 'derived' %}
            {% set metric_model_name = metrics.get_metric_model_name(metric_model=metric_definition.model) %}
            {% do metrics_dictionary_dict.update({'metric_model': metrics.get_model_relation(metric_model_name, metric_name)}) %}
        {% endif %}

    {% endif %}

    {% do return(metrics_dictionary_dict) %}

{% endmacro %}