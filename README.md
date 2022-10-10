# data-apps-ecommerce-template

[RudderStack](https://www.rudderstack.com/) has various tracking plans, one of which is [e-commerce event tracking](https://www.rudderstack.com/docs/event-spec/ecommerce-events-spec/). In this [dbt project](https://www.getdbt.com/product/what-is-dbt/), we assume that the event stream sdk is implemented with the tracking plan recommended by RudderStack. With that assumption, we generate various user featuers in this dbt project. The output of this project is a table in warehouse, with one row per user, containing different attributes/features of the user. The featuers come from five groups at a high level.
The complete list of the features is listed below:

### Features List

- traits features: These are related to user, which do not, or rarely change. 
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
- Engagement features: These are related to the time a user spends on the website/app.
    1. days_since_last_seen (int)
    2. is_churned_n_days (bool)
    3. active_days_in_past_n_days (int)
    4. Session based:  (a new session begins when the time since previous session becomes > n min, with n being a variable)
        1. avg_session_length_last_week (float)
        2. n_sessions_last_week (int)  
        3. avg_session_length_overall (float)
        4. n_sessions_overall (int)
- Revenue based features: These are related to the amount users spend
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
- Pre-revenue features: Related to the pre-checkout engagement features such as cart adds, abandoned carts etc
    1. carts_in_past_n_days
    2. total_carts
    3. products_added_in_past_n_days (both purchased and not purchased)
    4. total_products_added_to_cart
    5. days_since_last_cart_add (int)
- SKU based features: Related to the SKU, categories etc. 
    1. items_purchased_ever (List of unique product items, List[str])
    2. categories_purchased_ever (List of unique category ids, List[str])
    3. highest_spent_category (str) (based on the price of the products)
    4. highest_transacted_category (str)

### Prerequisites:
1. Event-stream sdk implemented following e-commerce event spec
2. [RudderStack ID stitch dbt](https://hub.getdbt.com/rudderlabs/id_stitching/latest/) is enabled and an id graph table is generated in the warehouse

### Setting up

As long as the events are instrumented following the e-commerce spec, no changes in the sql files are required. All the changes can be done from the `dbt_project.yml` file. The key variables that require to be changed are:

```


vars:
  event_stream_database: 'ANALYTICS_DB'  #This is the name of database
  event_stream_schema: 'DATA_APPS_SIMULATED'     #This is the name of schema
  start_date: '2000-01-01'              #This is the lower bound on date. Only events after this date would be considered. Typically used to ignore data during test setup days. 
  end_date: 'now'                #This is the upper bound on date .Default is 'now'; It can be modified to some old date to create snapshot of features as of an older timestamp
  date_format: 'YYYY-MM-DD'             # This is the date format
  session_cutoff_in_sec: 1800           # A session is a continuous sequence of events, as long as the events are within this interval. If two consecutive events occur at an interval of greater than this, a new session is created.
  lookback_days: [7,30,90,365]          #These are the days upto which the lookback features will be computed
  product_ref_var: 'product_id'         #This is the name of the property in the tracks calls that uniquely corresponds to the product
  category_ref_var: 'category_l1'       #This is the name of the property in the tracks calls that corresponds to the product category

```

Along with the above variables, the table names (variables that start with `tbl_` prefix) may need to be changed depending on the schema. 


### Output:
The project creates two main output tabels, `event_stream_customer_features` and `event_stream_feature_table`;  The `event_stream_feature_table` has features in long format, with following columns: `user_id, timestamp, feature_name, feature_type, feature_value_<numeric|string|array>`; The same data is presented in a wide form (pivoted data) in `event_stream_customer_features` table with one row per user_id for a given timestamp. 

### Point-in-time correctness
The `end_date` variable in `dbt_project.yml` can be used to generate the features as of some time in the past. The default value is `'now'` which generates the features as of the run timestamp. But if it is modified to some date in the past, only those events till that timestamp are considered to generate the features. This is a valuable functionality while generating training data for various Machine Learning models, which require historical data to train predictive models.


## Synthetic Data Creation
For an easy setup and trial, a synthetic dataset is supplied along with this project in the `seeds/` directory. 
It has simulated users' journeys of five users. This can be loaded in customer warehouse with the command [`dbt seed`](https://docs.getdbt.com/docs/building-a-dbt-project/seeds)


