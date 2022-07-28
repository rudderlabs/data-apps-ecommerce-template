# data-apps-ecommerce-template
## Synthetic Data Creation
Created a sample product catalogue.  
Created 5 users.  
Simulated the users' journeys following e-commerce specs.  

## Feature Store Generation
_**Currently available**_:<br>
Feature table for:<br>
+ user traits, obtained from stg_user_traits.sql<br>
+ engagement-based features (including session-based features), from stg_engagement_features.sql<br>
+ revenue-based features, from stg_revenue_features.sql<br>
+ pre-revenue based features, from stg_pre_revenue_features.sql<br>
+ SKU based features, from stg_sku_based_features.sql<br>
<br>

_**Master features table**_ can be obtained from event_stream_feature_table.sql<br>

Variables for referencing the products and their category can be edited in dbt_project.yaml<br>

NOTE: Limitation for Generating Features:<br>
Features which have '0' as the count will not show up in the table.

Find the complete feature list and description [here](https://www.notion.so/rudderstacks/e-commerce-feature-store-template-0a12e97a3c554b3c8b131850358a7d3e).