
with cte_gender as 
(select user_id, gender from 
(select user_id, properties_gender as gender, row_number() over(partition by user_id order by timestamp desc) as rn from 
{{ source('ecommerce', 'identifies') }} where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}'
and properties_gender is not null and properties_gender != '') a where rn = 1),
cte_age as ()
select coalesce(a.user_id, b.user_id) as user_id, gender, age from cte_gender a full outer join cte_age b on a.user_id = b.user_id