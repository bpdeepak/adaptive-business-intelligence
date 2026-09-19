-- Average order value per day = revenue / order_count from daily_revenue.
select
    r.date,
    r.revenue,
    r.order_count,
    case
        when r.order_count > 0 then round((r.revenue / r.order_count)::numeric, 2)
        else 0
    end as aov
from {{ ref('daily_revenue') }} r