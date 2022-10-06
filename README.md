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

List of features:

- traits features:
    1. gender (char)
    2. age_in_years (int)
    3. country (str)
    4. state (str)
    5. email_domain (str)
    6. has_mobile_app (bool)
    7. campaign_sources (Array[str])
    8. is_active_on_website (bool)
    9. days_since_account_creation (int)
    10. payment_modes <list of all payment modes ever (cod, credit card, debit card, wallet etc)> (List[str])
- Engagement features:
    1. days_since_last_seen (int)
    2. is_churned_n_days (bool) (This would act as a label for churn prediction model)
    3. active_days_in_past_n_days (int)
    4. Session based:  (a new session begins when the time since previous session becomes > n min, with n being a variable)
        1. avg_session_length_last_week (float)
        2. n_sessions_last_week (int)  
        3. avg_session_length_overall (float)
        4. n_sessions_overall (int)
- Revenue based features:
    1. amt_spent_in_past_n_days (float)
    2. total_amt_spent (float)
    3. last_transaction_value (float)
    4. average_transaction_value (float)
    5. average_units_per_transaction (float)
    6. median_transaction_value (float)
    7. highest_transaction_value (float)
    8. days_since_last_purchase (int)
    9. transactions_in_past_n_days (int)
    10. total_transactions (int)
    11. has_credit_card (bool)
- Pre-revenue features:
    1. carts_in_past_n_days
    2. total_carts
    3. products_added_in_past_n_days (both purchased and not purchased)
    4. total_products_added_to_cart
    5. days_since_last_cart_add (int)
- SKU based features:
    1. items_purchased_ever (List of unique product items, List[str])
    2. categories_purchased_ever (List of unique category ids, List[str])
    3. highest_spent_category (str) (based on the price of the products)
    4. highest_transacted_category (str)

Find the complete feature list and description [here](https://www.notion.so/rudderstacks/e-commerce-feature-store-template-0a12e97a3c554b3c8b131850358a7d3e).