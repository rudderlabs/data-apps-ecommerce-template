with cte_id_stitched_tracks as 
(select distinct b.main_id as main_id, timestamp from {{ source('ecommerce', 'tracks') }} a left join 
ANALYTICS_DB.DATA_APPS_SIMULATED.{{var('id_stitcher_name')}} b 
on (a.user_id = b.other_id and b.other_id_type = 'user_id'))
select main_id, datediff(day, date(max(timestamp)),date({{get_end_timestamp()}})) as days_since_last_seen
from cte_id_stitched_tracks
where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}' and main_id is not null
group by main_id