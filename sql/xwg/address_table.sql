-- For populating and materialising a table of addresses, values given as paramaters.
{% for address in addresses %}

   SELECT {{address}} AS address

   {% if not loop.last %}

   UNION

   {% endif %}

{% endfor %}
