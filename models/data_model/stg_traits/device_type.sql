select user_id, device_type from 
(select user_id, context_device_type as device_type, row_number() over(partition by user_id order by timestamp desc) as rn from 
{{ source('ecommerce', 'identifies') }} where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}'
and device_type is not null and device_type != '') where rn = 1