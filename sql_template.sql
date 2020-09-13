{% from 'macro.sql' import macro_join_condition %}
{% from 'macro.sql' import macro_filter_condition %}

CREATE OR REPLACE VIEW {{ params.viewName }}
AS
SELECT
    {{ params.viewColumnList|join(',') }}
FROM
    {{ params.baseTables|join(',') }}
WHERE
    {{ macro_filter_condition(params.filterConditions) }}
    {% if params.tableCount > 1 %}
        {{ macro_join_condition(params.tableL, params.tableR, params.joinKey)}}
    {% endif %}