-- Feature store for the demand-forecast model (Phase 2, predictive layer;
-- corrected in Phase 5). The features are built by
-- dbt/macros/forecast_features.sql, shared with feature_forecast_next_week (the
-- scoring mart), so training and serving compute every feature the same way.
--
-- Grain: one row per (product_category, week_start) over a dense weekly spine
-- that ends at the last COMPLETE week (the data's extraction-cutoff tail is
-- dropped). Every model feature is a lag or trailing window of prior weeks;
-- avg_order_value is the week's own AOV and is kept as a descriptive column
-- only (it is NOT a model feature: using it would leak the target).
{{ forecast_feature_rows(extra_weeks=0) }}

select
    category,
    week_start,
    revenue,
    orders,
    avg_order_value,
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
    series_weeks,
    case when revenue_lag1 is null then false else true end as has_prior_week
from featured
order by category, week_start
