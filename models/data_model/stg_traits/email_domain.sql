select user_id, email_domain from 
(select user_id,{{get_domain_from_email('properties_email')}} as email_domain, row_number() over(partition by user_id order by timestamp desc) as rn from 
{{ source('ecommerce', 'identifies') }} where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}'
and properties_email is not null and properties_email != '') where rn = 1