{% test non_negative(model, column_name) %}

-- TODO: Implementar el test para verificar que los valores en la columna son no negativos y no nulos.

select
    {{ column_name }}
from {{ model }}
where {{ column_name }} < 0 or {{ column_name }} is null

{% endtest %}


{% test not_future_date(model, column_name) %}
    -- Test 2: Verifica que la fecha no sea mayor al día de hoy.
    -- Útil para evitar registros con fechas erróneas en el futuro.
    select
        {{ column_name }}
    from {{ model }}
    where {{ column_name }} > current_date
{% endtest %}


{% test not_empty_string(model, column_name) %}
    -- Test 3: Verifica que un campo de texto no sea una cadena vacía o solo espacios.
    -- Diferente al not_null nativo, esto atrapa '' o '  '.
    select
        {{ column_name }}
    from {{ model }}
    where trim({{ column_name }}) = ''
{% endtest %}
