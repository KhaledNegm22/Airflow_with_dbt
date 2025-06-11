{{ config(materialized='table') }}

SELECT
    -- Degenerate Dimension / Primary Key for Fact Table
    s.id AS sale_id,

    -- Foreign Key to fact_leads (Degenerate Dimension)
    -- Assuming lead_id is an integer in your source sales table and acts as a direct link to fact_leads PK
    s.lead_id,

    -- Foreign Keys to Dimension Tables
    -- dim_property_type (from your dim_property_type model)
    dpt.property_type_id AS property_type_id, 

    -- Date Keys (using TO_CHAR for direct lookup as discussed)
    TO_CHAR(s.date_of_reservation, 'YYYYMMDD')::INT AS date_of_reservation_id,
    TO_CHAR(s.date_of_reservation::TIME, 'HH24MI')::INT AS time_of_reservation_id, -- Assuming HHMM for time_id
    TO_CHAR(s.reservation_update_date, 'YYYYMMDD')::INT AS reservation_update_id,
    TO_CHAR(s.reservation_update_date::TIME, 'HH24MI')::INT AS time_of_reservation_update_id,
    TO_CHAR(s.date_of_contraction, 'YYYYMMDD')::INT AS date_of_contraction_id,
    TO_CHAR(s.date_of_contraction::TIME, 'HH24MI')::INT AS time_of_contraction_id,

    -- dim_sale_category (from your dim_sale_category model)
    dsc.sale_category_id AS sale_category_id, -- Note: This is now category_id from your dim_sale_category model

    -- dim_sales_location (from your dim_sales_location model)
    dsl.unit_location_id AS location_id, -- Note: This is now unit_location_id from your dim_sales_location model

    -- Degenerate Dimensions (Attributes directly on Fact table, as per your schema)
    s.area_id,
    s.compound_id,

    -- Measures
    s.unit_value::DECIMAL(15,2) AS unit_value, -- Cast to DECIMAL(13,2)
    s.expected_value::DECIMAL(15,2) AS expected_value,
    s.actual_value::DECIMAL(15,2) AS actual_value,
    s.years_of_payment

FROM {{ source('data_source', 'sales') }} s -- Reference to your raw sales data

-- Joins to Dimension Tables to get Surrogate Keys

-- Join to dim_property_type
LEFT JOIN {{ ref('dim_property_type') }} dpt
    ON s.property_type = dpt.property_type -- Join on the natural key 'property_type'

-- Join to dim_sale_category
LEFT JOIN {{ ref('dim_sale_category') }} dsc
    ON s.sale_category = dsc.sale_category
-- Join to dim_sales_location
LEFT JOIN {{ ref('dim_sales_location') }} dsl
    ON s.unit_location = dsl.unit_location -- Join on the natural key 'unit_location'

WHERE s.id IS NOT NULL -- Ensure valid sales records