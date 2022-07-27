select * from {{ref('stg_user_traits')}}
union
select * from {{ref('stg_engagement_features')}}
union
select * from {{ref('stg_pre_revenue_features')}}
union
select * from {{ref('stg_revenue_features')}}
union
select * from {{ref('stg_sku_based_features')}}