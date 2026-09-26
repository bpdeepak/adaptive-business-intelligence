-- Scoring mart for the demand forecast (Phase 5): one row per category for the
-- week AFTER the last complete week of the data (its outcome does not exist).
--
-- Same macro as the training mart (dbt/macros/forecast_features.sql), so the
-- features are exactly what the model was trained on; they are all lags or
-- trailing windows of real weeks. recent_orders_avg / recent_revenue_avg are the
-- trailing 4-week realized means the purchase-order playbook compares the
-- forecast against. No now(): the reference point is the dataset's own last
-- complete week (as_of_week).
{{ forecast_feature_rows(extra_weeks=1) }}

select
    category,
    week_start,
    as_of_week,
    year,
    week_of_year,
    aov_lag1,
    revenue_lag1,
    revenue_lag2,
    revenue_lag4,
    revenue_lag8,
    orders_lag1,
    orders_lag2,
    orders_lag4,
    orders_lag8,
    revenue_roll4_mean,
    revenue_roll4_std,
    orders_roll4_mean,
    case when revenue_lag1 is null then false else true end as has_prior_week,
    orders_roll4_mean as recent_orders_avg,
    revenue_roll4_mean as recent_revenue_avg
from featured
where week_start > as_of_week
order by category
