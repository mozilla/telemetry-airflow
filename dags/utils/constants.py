DS_WEEKLY = (
    '{% if dag_run.external_trigger %}'
        '{{ ds_nodash }}'
    '{% else %}'
        '{{ macros.ds_format(macros.ds_add(ds, 7), "%Y-%m-%d", "%Y%m%d") }}'
    '{% endif %}'
)
