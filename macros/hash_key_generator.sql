 {% macro hash_key_generator(cols = []) %}
    SHA2(
    {%- for col in cols -%}
        {{ 'cast(trim(' ~  col.upper() ~ ') as string)'}}
        {%- if not loop.last -%}||'-'|| {% endif %}
    {% endfor -%}
    , 256)
{% endmacro %}