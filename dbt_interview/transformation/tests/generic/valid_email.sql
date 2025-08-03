
{% test valid_email (model,column_name) %}

select *
from {{model}}
where NOT REGEXP_MATCHES( {{column_name}}, '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')

{% endtest %}