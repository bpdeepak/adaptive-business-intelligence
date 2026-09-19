-- Olist's product table keeps the (misspelled) original column names.
select
    product_id,
    product_category_name,
    product_name_lenght as product_name_length,
    product_description_lenght as product_description_length,
    product_photos_qty::int as product_photos_qty,
    product_weight_g::int as product_weight_g,
    product_length_cm::int as product_length_cm,
    product_height_cm::int as product_height_cm,
    product_width_cm::int as product_width_cm,
    -- Products with every descriptive field null are placeholder/empty rows.
    case
        when product_category_name is null
             and product_name_lenght is null
             and product_description_lenght is null
             and product_photos_qty is null
             and product_weight_g is null
             and product_length_cm is null
             and product_height_cm is null
             and product_width_cm is null
            then true
        else false
    end as is_fully_empty
from {{ source('bronze', 'products') }}