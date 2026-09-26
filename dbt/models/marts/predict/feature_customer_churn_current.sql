-- Scoring mart for the churn model (Phase 5): the customers a retention
-- decision is actually open for.
--
-- Serving analogue of the training row: training asks "at a repeat customer's
-- order k, given only the orders before it, will they buy again within 90
-- days?" with k = the penultimate order. Here k = the customer's LATEST order,
-- the question the business is asking now. The features are the same macro
-- (dbt/macros/churn_features.sql), so there is no train/serve skew by
-- construction.
--
-- Population = exactly the rows the training mart's label-availability rule
-- excludes: repeat customers whose latest order falls inside the final 90 days
-- of the dataset, so their outcome is not yet observable. Customers whose
-- latest order is older already have a known outcome (they did not return), and
-- scoring them would be scoring history. There is no now() anywhere: the
-- dataset's own end is the reference point, which keeps the historical replay
-- honest.
with {{ churn_order_ctes() }},
asof as (
    select customer_unique_id, order_id as asof_order_id,
           order_purchase_date as as_of_date,
           order_purchase_timestamp as as_of_ts
    from ranked
    where n_orders >= 2 and ord_n = n_orders
),
{{ churn_feature_ctes() }}
select
    a.customer_unique_id,
    a.as_of_date,
    {{ churn_feature_columns() }}
from asof a
left join prior_agg p using (customer_unique_id, as_of_ts)
left join category_counts cc using (customer_unique_id, as_of_ts)
cross join data_end de
where a.as_of_ts > de.end_ts - interval '90 days'
order by a.customer_unique_id
