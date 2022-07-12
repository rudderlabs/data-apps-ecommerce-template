select user_id, gender from 
(select user_id, properties_gender as gender, row_number() over(partition by user_id order by timestamp desc) as rn from 
{{ source('ecommerce', 'identifies') }} where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}'
and properties_gender is not null and properties_gender != '') where rn = 1