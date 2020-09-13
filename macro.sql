{% macro macro_join_condition(l_tbl, r_tbls, column) %}
  {% for r_tbl in r_tbls %}
    AND {{ l_tbl }}.{{ column }} = {{ r_tbl }}.{{ column }}
  {% endfor %}
{% endmacro %}

{% macro macro_filter_condition(filters) %}
  {% for tbl_filter in filters %}
    {% if loop.first %}
      {{ tbl_filter }}
    {% else %}
      AND {{ tbl_filter }}
    {% endif %}
  {% endfor %}
{% endmacro %}