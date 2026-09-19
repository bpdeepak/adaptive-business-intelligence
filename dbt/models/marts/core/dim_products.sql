select
    p.product_id,
    p.product_category_name as category_name_pt,
    coalesce(c.category_name, 'unknown') as product_category,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    p.is_fully_empty
from {{ ref('stg_products') }} p
left join {{ ref('stg_category_translation') }} c
    on p.product_category_name = c.category_name_pt