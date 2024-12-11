{{ 
    config(
        materialized='table',
        snowflake_warehouse=get_snowflake_warehouse('DE_DBT_LARGE_WH'),
        description='Table for multi-class shopper fraud classification'
    )
}}

WITH filtered_labels AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY is_fraud ORDER BY RANDOM()) AS rn
        FROM {{ source('instadata_etl', 'fact_fulfillment_fraud_labels') }}
        WHERE data_source = 'sizing_hist'
          AND is_fraud IN (0, 1)
    ) AS label_sample
    WHERE is_fraud = 1 
       OR (is_fraud = 0 AND rn <= 20000)
),

fact_order_delivery_cte AS (
    SELECT DISTINCT
        fod.order_delivery_id,
        fod.batch_id,
        fod.delivery_created_date_time_utc,
        -- Delivery Times
        DATEDIFF('minute', fod.planned_delivery_start_date_time_utc, fod.planned_delivery_end_date_time_utc) AS planned_delivery_time,
        DATEDIFF('minute', fod.delivery_started_date_time_utc, fod.delivered_date_time_utc) AS actual_delivery_time,
        DATEDIFF('minute', fod.delivery_started_date_time_utc, fod.delivered_date_time_utc) - 
            DATEDIFF('minute', fod.planned_delivery_start_date_time_utc, fod.planned_delivery_end_date_time_utc) AS delivery_time_deviation,
        CASE 
            WHEN DATEDIFF('minute', fod.planned_delivery_start_date_time_utc, fod.planned_delivery_end_date_time_utc) = 0 THEN NULL
            ELSE DATEDIFF('minute', fod.delivery_started_date_time_utc, fod.delivered_date_time_utc) / NULLIF(DATEDIFF('minute', fod.planned_delivery_start_date_time_utc, fod.planned_delivery_end_date_time_utc), 0)
        END AS delivery_time_ratio,
        DATEDIFF('minute', fod.order_placed_date_time_utc, fod.delivery_started_date_time_utc) AS order_to_delivery_start_time,
        DATEDIFF('minute', fod.window_starts_date_time_utc, fod.window_ends_date_time_utc) AS delivery_window_duration,
        DATEDIFF('minute', fod.order_placed_date_time_utc, fod.delivered_date_time_utc) AS total_order_delivery_time,
        CASE 
            WHEN fod.delivery_canceled_date_time_utc IS NULL THEN 0 
            ELSE DATEDIFF('minute', fod.delivery_started_date_time_utc, fod.delivery_canceled_date_time_utc) 
        END AS delivery_canceled_time,
        fod.distance_from_home,    
        -- Financial Metrics
        fod.final_charge_amt / NULLIF(fod.num_items, 0) AS average_spend_per_item,
        CASE 
            WHEN fod.final_charge_amt = fod.initial_charge_amt THEN 0
            ELSE (fod.final_charge_amt - fod.initial_charge_amt) / NULLIF(fod.initial_charge_amt, 0)
        END AS overspend_ratio,
        -fod.cpg_delivery_promotion_discount_amt AS cpg_delivery_promotion_discount_amt,
        fod.delivery_promotion_discount_amt,
        fod.deal_discount_amt,
        (-fod.cpg_delivery_promotion_discount_amt + fod.delivery_promotion_discount_amt + fod.deal_discount_amt) AS total_discount_amt,
        CASE
            WHEN (fod.final_charge_amt - fod.initial_charge_amt) = 0 THEN 0
            ELSE 
                (-fod.cpg_delivery_promotion_discount_amt + fod.delivery_promotion_discount_amt + fod.deal_discount_amt) 
                / (fod.final_charge_amt - fod.initial_charge_amt)
        END AS discount_effectiveness_ratio, 
        fod.final_charge_amt / NULLIF(fod.total_charged_amt, 0) AS final_to_total_charged_ratio,
        (fod.admin_refund_amt / NULLIF(fod.final_charge_amt, 0)) AS admin_refund_ratio,
        (fod.final_charge_amt / NULLIF(fod.final_retailer_total_amt, 0)) AS charge_to_retailer_ratio,
        -- Tip Metrics
        fod.tip_amt,
        (fod.tip_amt - fod.initial_tip_amt) / NULLIF(fod.initial_tip_amt, 0) AS tip_change_ratio,
        fod.tip_amt / NULLIF(fod.final_charge_amt, 0) AS tip_percentage,
        
        -- Item Metrics
        fod.num_items,
        fod.num_refunded_items,
        fod.num_delivered_items,
        fod.num_refunded_items / NULLIF(fod.num_items, 0) AS refunded_item_ratio,
        (fod.num_refunded_items / NULLIF(fod.num_items, 0)) * (fod.final_charge_amt / NULLIF(fod.num_items, 0)) AS refund_impact,
        
        -- Delivery Details
        fod.delivery_type,
        fod.delivery_state,

        fod.is_unattended,
        fod.fulfillment_method,
        fod.region_id,

        -- Fraud Labels
        l.label_created_at_datetime_pt,
        CASE
            WHEN l.is_fraud = 0 THEN 'non-fraud'
            WHEN l.fraud_vector IN ('time_and_distance', 'distance_abuse') THEN 'time_and_distance'
            WHEN l.fraud_vector IN ('pre_checkout_unassign_full', 'pre_checkout_unassign_partial') THEN 'pre_checkout_unassign'
            WHEN l.fraud_vector IN ('post_checkout_unassign_full', 'post_checkout_unassign_partial') THEN 'post_checkout_unassign'
            WHEN l.fraud_vector = 'overspend' THEN 'overspend'
            WHEN l.fraud_vector IN ('oos_partial', 'oos_full', 'oos_cancellation_abuse') THEN 'oos'
            WHEN l.fraud_vector = 'earnings_abuse_bumps' THEN 'earnings_abuse_bumps'
            ELSE NULL
        END AS fraud_vector
    FROM 
        {{ source('instadata_dwh', 'fact_order_delivery') }} AS fod
    INNER JOIN 
        filtered_labels l 
        ON fod.order_delivery_id = l.order_delivery_id
    WHERE 
        fod.whitelabel_id = 1
        AND fod.delivery_created_date_time_utc >= DATEADD(month, -18, CURRENT_DATE)
        AND l.fraud_vector IN (
            'time_and_distance', 
            'distance_abuse', 
            'pre_checkout_unassign_full', 
            'pre_checkout_unassign_partial',
            'post_checkout_unassign_full', 
            'post_checkout_unassign_partial',
            'overspend',
            'oos_partial', 
            'oos_full', 
            'oos_cancellation_abuse',
            'earnings_abuse_bumps',
            'non-fraud'
        )
),

-- Combined CTE for LOGISTICS_BATCHES and PICKING_SETS
combined_batches_cte AS (
    SELECT DISTINCT
        lb.batch_id,
        -- Time and Distance Metrics from logistics_batches
        COALESCE(lb.delivery_actual_distance / NULLIF(lb.delivery_offered_distance_miles, 0), 0) AS batch_delivery_distance_ratio,
        COALESCE(lb.driving_actual_distance / NULLIF(lb.driving_offered_distance_miles, 0), 0) AS batch_driving_distance_ratio,
        COALESCE(
            lb.driving_actual_distance * 1.60934 / NULLIF(DATEDIFF('minute', ps.delivery_started_at, ps.delivery_completed_at), 0),
            0
        ) AS batch_average_delivery_speed,
        
        -- Batch Details
        lb.batch_type,
        lb.picking_time_buffer,
        
        -- Picking Metrics
        DATEDIFF('minute', ps.picking_started_at, ps.picking_completed_at) / NULLIF(SUM(fod.num_items), 0) AS batch_avg_picking_time_per_item,
        DATEDIFF('minute', ps.delivery_started_at, ps.delivery_completed_at) AS batch_actual_delivery_time,
        DATEDIFF('minute', ps.planned_delivery_started_at, ps.delivery_started_at) AS batch_delay_starting_delivery,
        DATEDIFF('minute', ps.picking_completed_at, ps.delivery_started_at) AS batch_idle_time_before_delivery,
        DATEDIFF('minute', ps.created_at, ps.delivery_acknowledged_at) AS batch_acknowledgement_time,
        DATEDIFF('minute', ps.created_at, ps.delivery_completed_at) AS batch_total_order_duration,
        DATEDIFF('minute', ps.delivery_started_at, ps.delivery_completed_at) - 
            DATEDIFF('minute', ps.planned_delivery_started_at, ps.planned_delivery_completed_at) AS batch_delivery_time_deviation,
        COALESCE(
            DATEDIFF('minute', ps.picking_started_at, ps.picking_completed_at) / NULLIF(DATEDIFF('minute', ps.delivery_started_at, ps.delivery_completed_at), 0),
            0
        ) AS batch_picking_to_delivery_ratio,
        DATEDIFF('minute', ps.picking_started_at, ps.picking_completed_at) / 
            NULLIF(DATEDIFF('minute', ps.planned_picking_started_at, ps.planned_delivery_started_at), 0) AS batch_picking_time_ratio,
        CASE 
            WHEN ps.suggested_warehouse_location_id = ps.warehouse_location_id THEN 1 
            ELSE 0 
        END AS is_warehouse_location_similar
    FROM {{ source('instadata_rds_data', 'logistics_batches') }} AS lb
    LEFT JOIN {{ source('instadata_rds_data', 'picking_sets') }} AS ps ON lb.batch_id = ps.id
    LEFT JOIN {{ source('instadata_dwh', 'fact_order_delivery') }} AS fod ON ps.id = fod.batch_id
    GROUP BY lb.batch_id, ps.id, lb.delivery_actual_distance, lb.delivery_offered_distance_miles, lb.driving_actual_distance, lb.driving_offered_distance_miles, lb.estimated_wage_cents, lb.picking_effort_mpi, lb.batch_type, lb.picking_time_buffer, ps.delivery_started_at, ps.planned_delivery_started_at, ps.picking_started_at, ps.picking_completed_at, ps.delivery_completed_at, ps.planned_delivery_completed_at, ps.delivery_acknowledged_at, ps.created_at, ps.planned_picking_started_at, ps.suggested_warehouse_location_id, ps.warehouse_location_id
),

-- CTE for SHOPPER_ORDER_ITEMS (Aggregated to Order-Level)
shopper_order_items_cte AS (
    SELECT
        soi.order_delivery_id,
        SUM(soi.price) AS total_price
    FROM 
        {{ source('rds_picking', 'shopper_order_items') }} AS soi
    GROUP BY 
        soi.order_delivery_id
),

-- CTE for SHOPPER_FRAUD_ANOMALY_DETECTION_FEATURES
shopper_fraud_anomaly_detection_features_cte AS (
    SELECT DISTINCT
        adf.order_delivery_id,
        adf.shopper_id,
        adf.delta_actual_vs_predicted_mileage,
        adf.picking_time_per_item_seconds / 60 AS picking_time_per_item_minutes,
        adf.tip_delta,
        adf.delivery_time_seconds / 60 AS delivery_time_minutes,
        adf.found_item_ratio,
        adf.replaced_item_ratio,
        adf.driving_distance_miles,
        adf.batch_actual_mileage
    FROM {{ source('instadata_etl', 'shopper_anomaly_detection_features_m1_agg') }} AS adf
),

fact_fulfillment_order_delivery_pos_txn_api_cte AS (
    SELECT DISTINCT
        ffodp.order_delivery_id,
        ffodp.shopper_id,
        ffodp.batch_id,
        ffodp.num_successful_swipes / NULLIF(ffodp.num_swipes, 0) AS successful_swipe_ratio,
        ffodp.num_declined_swipes / NULLIF(ffodp.num_swipes, 0) AS declined_swipe_ratio,
        ffodp.latest_swipe_amount - ffodp.amt_successful_swipe_total AS delta_swipe_amount,
        ffodp.amt_declined_swipe_total / NULLIF(ffodp.amt_successful_swipe_total, 0) AS amt_declined_ratio,
        DATEDIFF('minute', ffodp.first_swipe_datetime_pt, ffodp.latest_swipe_datetime_pt) AS swipe_time_diff_minutes
    FROM {{ source('instadata_etl', 'fact_fulfillment_order_delivery_pos') }} AS ffodp
),

drivers_cte AS (
    SELECT DISTINCT
        d.id,
        d.reliability_score,
        d.experienced_shopper,
        d.driver_delivery_multiple,
        d.has_vehicle,
        d.has_active_card,
        d.percentile_rank
    FROM {{ source('instadata_rds_data', 'drivers') }} AS d
),

-- CTE for FACT_FULFILLMENT_FRAUD_WAGES
fact_fulfillment_fraud_wages_cte AS (
    SELECT DISTINCT
        fffw.order_delivery_id,
        SUM(fffw.amt_delivered_wages_total) / NULLIF(SUM(fffw.amt_wages_total), 0) AS delivered_to_total_wages_ratio,
        SUM(fffw.amt_wages_shopper_bump) / NULLIF(SUM(fffw.amt_wages_total), 0) AS adjusted_bonus_ratio,
        SUM(fffw.amt_deactivated_wages_total) AS total_amt_deactivated_wages_total,       
        SUM(fffw.amt_deactivated_wages_total) / NULLIF((SUM(fffw.amt_deactivated_wages_total) + SUM(fffw.amt_delivered_wages_total)), 0) AS cancellation_to_completion_ratio,
        SUM(fffw.amt_wages_tip) / NULLIF(SUM(fffw.amt_wages_other), 0) AS tip_to_other_wage_ratio,
        MAX(fffw.has_return_bump) AS has_return_bump
    FROM {{ source('instadata_etl', 'fact_fulfillment_fraud_wages') }} AS fffw
    GROUP BY fffw.order_delivery_id
),

final_table AS (
    SELECT DISTINCT
        fod.*,
        lb.* EXCLUDE(batch_id),
        adf.* EXCLUDE(order_delivery_id, shopper_id),
        soi.* EXCLUDE(order_delivery_id),
        fffw.* EXCLUDE(order_delivery_id),
        ffodp.* EXCLUDE(order_delivery_id, batch_id),
        d.* EXCLUDE(id)
    FROM fact_order_delivery_cte fod
    LEFT JOIN combined_batches_cte lb ON fod.batch_id = lb.batch_id
    LEFT JOIN fact_fulfillment_fraud_wages_cte fffw ON fod.order_delivery_id = fffw.order_delivery_id
    LEFT JOIN shopper_order_items_cte soi ON fod.order_delivery_id = soi.order_delivery_id
    LEFT JOIN shopper_fraud_anomaly_detection_features_cte adf ON fod.order_delivery_id = adf.order_delivery_id
    LEFT JOIN fact_fulfillment_order_delivery_pos_txn_api_cte ffodp ON fod.order_delivery_id = ffodp.order_delivery_id
    LEFT JOIN drivers_cte d ON ffodp.shopper_id = d.id
)

SELECT * FROM final_table