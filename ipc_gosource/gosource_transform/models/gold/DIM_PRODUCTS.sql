{{ config(materialized='table', schema='gold', tags=['Python_Gosource']) }}

WITH customer_details AS (
    SELECT 
        p.product_id_pk, 
        p.product_name,
        CASE 
            WHEN p.product_brand IN ('Generic', 'Market') THEN NULL 
            ELSE p.product_brand 
        END AS product_brand,  -- Replace 'generic' and 'market' with NULL
        p.product_category_fk,
        c.categories_name,
        p.product_unit_key,
        p.product_unit_value,
        
        -- Concatenate product name and brand in parentheses if the brand is not NULL
        CASE 
            WHEN p.product_brand IN ('Generic', 'Market') OR p.product_brand IS NULL THEN p.product_name
            ELSE p.product_name || ' (' || p.product_brand || ' ' || p.product_unit_key || ')'
        END AS product_name_with_brand
        
    FROM {{ ref('bv_products') }} p
    JOIN {{ ref('bv_categories') }} c 
        ON c.categories_id_pk = p.product_category_fk
)

SELECT * FROM customer_details
