-- aov_lag1 must be LAST week's AOV, never the same week's (the leak it replaces).
select w.category, w.week_start
from {{ ref('feature_forecast_weekly') }} w
join {{ ref('feature_forecast_weekly') }} prev
  on prev.category = w.category and prev.week_start = w.week_start - 7
where w.aov_lag1 is distinct from prev.avg_order_value
