{{ config(materialized='table') }}

SELECT
    -- Degenerate Dimension / Primary Key for Fact Table
    l.id AS lead_id,

    -- Foreign Keys to Dimensions
    -- dim_customer
    l.customer_id, -- This is the surrogate key from dim_customer

    -- Date and Time Keys for various lead dates
    TO_CHAR(l.date_of_last_request, 'YYYYMMDD')::INT AS last_request_date_id,
    TO_CHAR(l.date_of_last_request::TIME, 'HH24MI')::INT AS last_request_time_id,
    TO_CHAR(l.created_at, 'YYYYMMDD')::INT AS created_at_date_id,
    TO_CHAR(l.created_at::TIME, 'HH24MI')::INT AS created_at_time_id,
    TO_CHAR(l.updated_at, 'YYYYMMDD')::INT AS updated_at_date_id,
    TO_CHAR(l.updated_at::TIME, 'HH24MI')::INT AS updated_at_time_id,
    -- TO_CHAR(l.date_of_last_contact, 'YYYYMMDD')::INT AS last_contact_date_id, -- Column name in source is 'date_of_last_contact'
    -- TO_CHAR(l.date_of_last_contact::TIME, 'HH24MI')::INT AS last_contact_time_id, -- Column name in source is 'date_of_last_contact'
    -- Corrected column names based on source for date_of_last_contact
    TO_CHAR(l.date_of_last_contact, 'YYYYMMDD')::INT AS last_contact_date_id,
    TO_CHAR(l.date_of_last_contact::TIME, 'HH24MI')::INT AS last_contact_time_id,

    -- dim_user (assuming user_id from source is directly used as FK to dim_user's PK)
    l.user_id AS user_id, -- As per your schema, user_id is directly on fact_leads

    -- dim_status
    ds.status_id AS status_id,
    -- dim_method_of_contact
    dmoc.method_id AS method_of_contact_id,
    -- dim_campaign
    dcmp.campaign_id AS campaign_id,
    -- dim_lead_type
    dlt.lead_type_id AS lead_type_id,
    -- dim_lead_sources
    dls.lead_source_id AS lead_source_id,

    -- Measures
    l.budget::DECIMAL(15,2) AS budget,
    -- Degenerate Dimensions (Attributes directly on Fact table)
    l.location AS location, -- As per your schema, location is directly on fact_leads
    l.area_id AS area_id,
    l.compound_id AS compound_id,
    l.developer_id AS developer_id,
    l.meeting_flag AS meeting_flag,
    l.commercial AS commercial,
    l.merged AS merged,
    l.do_not_call AS do_not_call,
    -- Add lead_count measure
    1 AS lead_count

FROM {{ source('data_source', 'leads') }} l -- Reference to your raw leads data

-- Joins to Dimension Tables to get Surrogate Keys

-- dim_customer (assuming customer_id from source is directly used as FK to dim_customer's PK)

-- dim_status
LEFT JOIN {{ ref('dim_status') }} ds
    ON l.status_name = ds.status_name

-- dim_method_of_contact
LEFT JOIN {{ ref('dim_method_of_contact') }} dmoc
    ON l.method_of_contact = dmoc.method_of_contact

-- dim_campaign
LEFT JOIN {{ ref('dim_campaign') }} dcmp
    ON l.campaign = dcmp.campaign

-- dim_lead_type
LEFT JOIN {{ ref('dim_lead_type') }} dlt
    ON l.lead_type = dlt.lead_type

-- dim_lead_sources (careful with the mixed data types in source.lead_source)
LEFT JOIN {{ ref('dim_lead_source') }} dls
    ON l.lead_source = dls.lead_source  -- Cast to VARCHAR for join if source has mixed types

WHERE l.id IS NOT NULL -- Ensure valid lead records