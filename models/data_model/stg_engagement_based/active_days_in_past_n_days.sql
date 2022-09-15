with cte_id_stitched_tracks as (

    select distinct b.{{ var('main_id') }}, 
    {{ var('col_ecommerce_tracks_timestamp') }} 
    from {{ var('tbl_ecommerce_tracks') }} a 
    left join {{ var('tbl_id_stitcher')}} b 
    on (a.{{ var('col_ecommerce_tracks_user_id')}} = b.{{ var('col_id_stitcher_other_id')}} and b.{{ var('col_id_stitcher_other_id_type')}} = 'user_id')

), cte_active_days_in_past_n_days as (

    {% for lookback_days in var('lookback_days') %}
        select {{ var('main_id') }},
        count(distinct date({{ var('col_ecommerce_tracks_timestamp') }})) as active_days_in_past_n_days,
        {{lookback_days}} as n_value
        from cte_id_stitched_tracks
        where datediff(day, date({{ var('col_ecommerce_tracks_timestamp') }}), date({{get_end_timestamp()}})) <= {{lookback_days}} and {{timebound( var('col_ecommerce_tracks_timestamp'))}} and {{ var('main_id')}} is not null
        group by {{ var('main_id') }}
    {% if not loop.last %} union {% endif %}
    {% endfor %}
)

{% for lookback_days in var('lookback_days') %}
select {{ var('main_id') }}, 
    'active_days_in_past_{{lookback_days}}_days' as feature_name, active_days_in_past_n_days as feature_value 
from cte_active_days_in_past_n_days 
where n_value = {{lookback_days}}
{% if not loop.last %} union {% endif %}
{% endfor %}