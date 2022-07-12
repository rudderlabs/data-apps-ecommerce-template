select user_id, is_member from
(select user_id, 
(CASE WHEN properties_is_member = TRUE
      THEN TRUE
      ELSE FALSE
END) as is_member, row_number() over(partition by user_id order by timestamp desc) as rn from 
{{ source('ecommerce', 'identifies') }} where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}'
and user_id is not null) where rn = 1