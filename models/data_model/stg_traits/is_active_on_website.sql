select user_id, is_active_on_website from 
(select user_id, 
(CASE WHEN user_id IN(select user_id from {{ source('ecommerce', 'identifies') }} where context_device_type like '%PC')
      THEN TRUE
      ELSE FALSE
      END)
as is_active_on_website, row_number() over(partition by user_id order by timestamp desc) as rn from 
{{ source('ecommerce', 'identifies') }} where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}'
and context_device_type is not null and context_device_type != '' and user_id is not null) where rn = 1