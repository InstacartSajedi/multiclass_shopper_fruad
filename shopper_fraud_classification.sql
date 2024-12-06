{{ 
    config(
        materialized='table',
        description='table for multi-class shopper fraud classification'
    ) 
}}-- CTE for FACT_ORDER_DELIVERY
WITH fact_order_delivery_cte AS (
    SELECT
        -- Transaction features
        fod.order_delivery_id,
        fod.batch_id,
        fod.delivery_created_date_time_utc,
        fod.delivered_date_time_utc,
        fod.initial_charge_amt,
        fod.final_charge_amt,
        fod.total_charged_amt,
        (fod.final_charge_amt - fod.initial_charge_amt) / NULLIF(fod.initial_charge_amt, 0) AS overspend_ratio,
        fod.final_charge_amt / NULLIF(fod.num_items, 0) AS average_spend_per_item,
        fod.cpg_delivery_promotion_discount_amt,
        fod.delivery_promotion_discount_amt,
        fod.deal_discount_amt,
        fod.cpg_delivery_promotion_discount_amt + fod.delivery_promotion_discount_amt + fod.deal_discount_amt AS total_discount_amt,
        (fod.cpg_delivery_promotion_discount_amt + fod.delivery_promotion_discount_amt + fod.deal_discount_amt) / NULLIF((fod.final_charge_amt - fod.initial_charge_amt), 0) AS discount_effectiveness_ratio,
        -- Time and distance features
        fod.delivery_timing_minutes,
        DATEDIFF('minute', fod.planned_delivery_start_date_time_utc, fod.planned_delivery_end_date_time_utc) AS planned_delivery_time,
        fod.delivery_timing_minutes / NULLIF(DATEDIFF('minute', fod.planned_delivery_start_date_time_utc, fod.planned_delivery_end_date_time_utc), 0) AS delivery_efficiency,
        DATEDIFF('minute', fod.delivery_started_date_time_utc, fod.delivery_canceled_date_time_utc) AS delivery_canceled_time,
        fod.distance_from_home,
        -- Compensation features
        fod.tip_amt,
        fod.initial_tip_amt,
        (fod.tip_amt - fod.initial_tip_amt) / NULLIF(fod.initial_tip_amt, 0) AS tip_change_ratio,
        -- Item features
        fod.num_items,
        fod.num_refunded_items,
        fod.num_unresolved_items,
        fod.num_delivered_items,
        fod.num_refunded_items / NULLIF(fod.num_items, 0) AS refunded_item_ratio,
        fod.num_unresolved_items / NULLIF(fod.num_items, 0) AS unresolved_item_ratio,
        (fod.num_refunded_items / NULLIF(fod.num_items, 0)) * (fod.final_charge_amt / NULLIF(fod.num_items, 0)) AS refund_impact,
        -- Other features:
        fod.delivery_type,
        fod.delivery_finalized_ind,
        fod.delivery_state,
        fod.customer_contacted_successfully_ind,
        fod.num_user_canceled_items,
        fod.is_unattended,
        fod.fulfillment_method,
        fod.final_retailer_total_amt,
        fod.admin_refund_amt,
        fod.whitelabel_id
    FROM {{ source('instadata_dwh', 'fact_order_delivery') }} AS fod
    WHERE TRUE
    -- Filter out non-marketplace orders
    AND whitelabel_id = 1
),

-- CTE for LOGISTICS_BATCHES
logistics_batches_cte AS (
    SELECT
        lb.batch_id,
        lb.delivery_actual_distance,
        lb.delivery_offered_distance_miles,
        COALESCE(lb.delivery_actual_distance / NULLIF(lb.delivery_offered_distance_miles, 0), 0) AS delivery_distance_ratio,
        lb.driving_actual_distance,
        lb.driving_offered_distance_miles,
        COALESCE(lb.driving_actual_distance / NULLIF(lb.driving_offered_distance_miles, 0), 0) AS driving_distance_ratio,
        -- Time features
        DATEDIFF('minute', lb.delivery_started_at, lb.delivery_completed_at) AS actual_delivery_time,
        COALESCE(DATEDIFF('minute', lb.picking_started_at, lb.delivery_started_at) / NULLIF(DATEDIFF('minute', lb.delivery_started_at, lb.delivery_completed_at), 0), 0) AS picking_to_delivery_ratio,
        COALESCE(lb.driving_actual_distance / NULLIF((DATEDIFF('minute', lb.delivery_started_at, lb.delivery_completed_at) / 60), 0), 0) AS average_delivery_speed,
        -- Compensation features
        lb.estimated_wage_cents,
        COALESCE(lb.estimated_wage_cents / NULLIF(lb.picking_effort_mpi, 0), 0) AS wage_to_effort_ratio,
        lb.picking_effort_mpi,
        lb.batch_type,
        lb.picking_time_buffer
    FROM {{ source('instadata_rds_data', 'logistics_batches') }} AS lb
),

-- CTE for FACT_FULFILLMENT_FRAUD
fact_fulfillment_fraud_cte AS (
    SELECT
        fff.batch_id,
        fff.order_delivery_id,
        fff.shopper_id,
        fff.overspend_amt_usd,
        -- fff.amt_successful_swipe_total,
        -- fff.amt_declined_swipe_total,
        fff.amt_successful_swipe_total / NULLIF((fff.amt_successful_swipe_total + fff.amt_declined_swipe_total), 0) AS transaction_success_ratio,
        fff.pre_checkout_unassign_full_fraud_costs + fff.pre_checkout_unassign_partial_fraud_costs AS pre_checkout_unassign_costs,
        fff.post_checkout_unassign_full_fraud_costs + fff.post_checkout_unassign_partial_fraud_costs AS post_checkout_unassign_costs,
        fff.oos_full_fraud_costs + fff.oos_partial_fraud_costs AS oos_costs,
        fff.time_and_distance_fraud_costs,
        fff.earnings_abuse_bumps_fraud_costs,
        fff.pre_checkout_unassign_full_fraud_costs + fff.pre_checkout_unassign_partial_fraud_costs + fff.oos_full_fraud_costs + fff.oos_partial_fraud_costs + fff.time_and_distance_fraud_costs AS total_loss_costs,
        fff.has_unassign_batch_bump,
        fff.order_cancellation_type,
        fff.reimbursement_amt,
        fff.cal_coupons_issued,
        fff.reimbursement_request_state
    FROM {{ source('instadata_etl', 'fact_fulfillment_fraud') }} AS fff
),

-- CTE for FACT_FULFILLMENT_FRAUD_WAGES
fact_fulfillment_fraud_wages_cte AS (
    SELECT
        fffw.batch_id,
        fffw.order_delivery_id,
        fffw.shopper_id,
        fffw.amt_wages_shopper_bump,
        fffw.amt_wages_total,
        fffw.amt_wages_shopper_bump / NULLIF(fffw.amt_wages_total, 0) AS adjusted_bonus_ratio,
        fffw.amt_deactivated_wages_total,
        fffw.amt_delivered_wages_total,
        fffw.amt_deactivated_wages_total / NULLIF((fffw.amt_deactivated_wages_total + fffw.amt_delivered_wages_total), 0) AS cancellation_to_completion_ratio,
        fffw.amt_wages_tip,
        fffw.amt_wages_other,
        fffw.amt_wages_tip / NULLIF(fffw.amt_wages_other, 0) AS tip_to_other_wage_ratio,
        fffw.has_canceled_order_bump,
        fffw.has_return_bump,
        fffw.amt_referral,
        fffw.amt_deactivated_wages_other,
        fffw.amt_deactivated_wages_shopper_bump,
        fffw.amt_deactivated_wages_tip
    FROM {{ source('instadata_etl', 'fact_fulfillment_fraud_wages') }} AS fffw
),

-- CTE for SHOPPER_ORDER_ITEMS
shopper_order_items_cte AS (
    SELECT
        soi.order_delivery_id,
        soi.picked_via,
        soi.status,
        soi.delivered_quantity,
        soi.ordered_quantity,
        COALESCE((soi.ordered_quantity - soi.delivered_quantity) / NULLIF(soi.ordered_quantity, 0), 0) AS quantity_change_ratio,
        soi.price,
        soi.substitute_type,
        soi.big_and_bulky
    FROM {{ source('rds_picking', 'shopper_order_items') }} AS soi
),

-- CTE for SHOPPER_FRAUD_ANOMALY_DETECTION_FEATURES
shopper_fraud_anomaly_detection_features_cte AS (
    SELECT
        adf.order_delivery_id,
        adf.shopper_id,
        adf.delta_actual_vs_predicted_mileage,
        adf.picking_time_per_item_seconds,
        adf.tip_delta,
        adf.delivery_time_seconds,
        adf.found_item_ratio,
        adf.replaced_item_ratio,
        --adf.refunded_item_ratio,
        adf.driving_distance_miles,
        adf.batch_actual_mileage,
        adf.batch_predicted_mileage
    FROM {{ source('instadata_etl', 'shopper_anomaly_detection_features_m1_agg') }} AS adf
),

-- CTE for PICKING_SETS
picking_sets_cte AS (
    SELECT
        ps.id,
        ps.picking_started_at,
        ps.picking_completed_at,
        ps.delivery_started_at,
        ps.delivery_completed_at,
        ps.planned_delivery_started_at,
        ps.planned_delivery_completed_at,
        ps.delivery_acknowledged_at,
        ps.created_at,
        -- ps.batch_type,
        ps.suggested_warehouse_location_id,
        ps.warehouse_location_id,
        ps.picking_estimate_1,
        ps.picking_estimate_2,
        -- Calculated fields
        DATEDIFF('minute', ps.picking_started_at, ps.picking_completed_at) AS actual_picking_time,
        2 * DATEDIFF('minute', ps.picking_started_at, ps.picking_completed_at) / NULLIF((ps.picking_estimate_1 + ps.picking_estimate_2), 0) AS picking_time_ratio,
        DATEDIFF('minute', ps.picking_started_at, ps.picking_completed_at) / NULLIF(fod.num_items, 0) AS avg_picking_time_per_item,
        DATEDIFF('minute', ps.delivery_started_at, ps.delivery_completed_at) AS delivery_time,
        DATEDIFF('minute', ps.delivery_started_at, ps.delivery_completed_at) - DATEDIFF('minute', ps.planned_delivery_started_at, ps.planned_delivery_completed_at) AS delivery_time_deviation,
        --DATEDIFF('minute', ps.planned_delivery_started_at, ps.planned_delivery_completed_at) AS planned_delivery_time,
        DATEDIFF('minute', ps.delivery_started_at, ps.planned_delivery_started_at) AS delay_starting_delivery,
        DATEDIFF('minute', ps.delivery_started_at, ps.picking_completed_at) AS idle_time_before_delivery,
        DATEDIFF('minute', ps.delivery_acknowledged_at, ps.created_at) AS acknowledgement_time,
        DATEDIFF('minute', ps.delivery_completed_at, ps.created_at) AS total_order_duration,
        -- DATEDIFF('minute', ps.delivery_started_at, ps.delivery_completed_at) / NULLIF(DATEDIFF('minute', ps.planned_delivery_started_at, ps.planned_delivery_completed_at), 0) AS delivery_efficiency
    FROM {{ source('instadata_rds_data', 'picking_sets') }} AS ps
    LEFT JOIN {{ source('instadata_dwh', 'fact_order_delivery') }} AS fod ON ps.id = fod.batch_id
),

fact_fulfillment_order_delivery_pos_txn_api_cte AS (
    SELECT
        ffodp.order_delivery_id,
        ffodp.shopper_id,
        ffodp.batch_id,
        ffodp.num_swipes,
        ffodp.num_successful_swipes,
        ffodp.num_declined_swipes,
        ffodp.latest_swipe_amount,
        ffodp.amt_successful_swipe_total,
        ffodp.amt_declined_swipe_total,
        ffodp.latest_swipe_result,
        -- Calculated fields
        ffodp.num_successful_swipes / NULLIF(ffodp.num_swipes, 0) AS successful_swipe_ratio,
        ffodp.num_declined_swipes / NULLIF(ffodp.num_swipes, 0) AS declined_swipe_ratio,
        ffodp.latest_swipe_amount - ffodp.amt_successful_swipe_total AS delta_swipe_amount,
        ffodp.amt_declined_swipe_total / NULLIF(ffodp.amt_successful_swipe_total, 0) AS amt_declined_ratio
    FROM {{ source('instadata_etl', 'fact_fulfillment_order_delivery_pos') }} AS ffodp
),

drivers_cte AS (
    SELECT
        d.id,
        d.reliability_score,
        d.experienced_shopper,
        d.perfect_deliveries_score,
        d.probation_state,
        d.driver_delivery_multiple,
        d.picking_multiple,
        d.has_vehicle,
        d.has_active_card,
        d.large_orders_enabled,
        d.percentile_rank,
        -- Calculated fields
        d.reliability_score * d.perfect_deliveries_score AS adjusted_reliability_index,
        -- d.reliability_score * d.perfect_deliveries_score * fod.delivery_efficiency AS shopper_consistency_score
    FROM {{ source('instadata_rds_data', 'drivers') }} AS d
    --LEFT JOIN {{ source('instadata_dwh', 'fact_order_delivery') }} AS fod ON d.id = fod.shopper_id
),

labels AS (
    SELECT
        l.label_created_at_datetime_pt,
        l.shopper_id,
        l.order_id,
        l.order_delivery_id,
        l.batch_id,
        l.label_grain,
        l.fraud_vector,
        CASE 
            WHEN l.is_fraud = -1 THEN NULL
            ELSE l.is_fraud
        END AS is_fraud,
        'sizing_hist' AS data_source
    FROM {{ source('instadata_etl', 'fact_fulfillment_fraud_labels') }} AS l
),

final_table AS (
    SELECT
        fod.*,
        lb.* EXCLUDE(batch_id),
        fff.* EXCLUDE(batch_id, order_delivery_id),
        sadf.* EXCLUDE(order_delivery_id, shopper_id),
        soi.* EXCLUDE(order_delivery_id),
        fffw.* EXCLUDE(batch_id, order_delivery_id, shopper_id),
        ffodp.* EXCLUDE(order_delivery_id, shopper_id, batch_id),
        ps.* EXCLUDE(id),
        d.* EXCLUDE(id),
        l.* EXCLUDE(order_delivery_id, shopper_id, batch_id)
    FROM fact_order_delivery_cte fod
    -- Keep all your existing joins
    LEFT JOIN logistics_batches_cte lb ON fod.batch_id = lb.batch_id
    LEFT JOIN fact_fulfillment_fraud_cte fff ON fod.batch_id = fff.batch_id
    LEFT JOIN picking_sets_cte ps ON fod.batch_id = ps.id
    LEFT JOIN fact_fulfillment_fraud_wages_cte fffw ON fod.order_delivery_id = fffw.order_delivery_id
    LEFT JOIN shopper_order_items_cte soi ON fod.order_delivery_id = soi.order_delivery_id
    LEFT JOIN shopper_fraud_anomaly_detection_features_cte sadf ON fod.order_delivery_id = sadf.order_delivery_id
    LEFT JOIN fact_fulfillment_order_delivery_pos_txn_api_cte ffodp ON fod.order_delivery_id = ffodp.order_delivery_id
    LEFT JOIN labels l ON fod.order_delivery_id = l.order_delivery_id
    LEFT JOIN drivers_cte d ON fff.shopper_id = d.id
)

SELECT * FROM final_table

