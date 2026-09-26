-- The scoring week is exactly one week after the last training week, for every
-- category, and never overlaps a training row.
select n.category, n.week_start
from {{ ref('feature_forecast_next_week') }} n
where n.week_start <> (select max(week_start) from {{ ref('feature_forecast_weekly') }}) + 7
   or exists (select 1 from {{ ref('feature_forecast_weekly') }} w
              where w.category = n.category and w.week_start = n.week_start)
