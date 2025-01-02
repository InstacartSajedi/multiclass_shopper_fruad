{{ config(
    materialized='table',
    snowflake_warehouse=get_snowflake_warehouse('DE_DBT_LARGE_WH'),
    description='Table with enriched features for shopper fraud classification with full non-fraud data'
) }}

WITH base_data AS (
    SELECT *
    FROM SANDBOX_DB.AHMADSAJEDI.shopper_fraud_classification_full
    WHERE order_delivery_id IS NOT NULL
),

aggregated_features AS (
    SELECT
        bd.order_delivery_id,
        bd.shopper_id,
        bd.delivery_created_date_time_utc,

        -- Aggregated features over the past 7 days
        COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '7 DAY'
                   AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.order_delivery_id END) AS num_orders_7_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '7 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.refunded_item_ratio END) AS avg_refund_ratio_7_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '7 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.picking_time_per_item_minutes END) AS avg_picking_time_per_item_7_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '7 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.overspend_ratio END) AS avg_overspend_ratio_7_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '7 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.refund_impact END) AS avg_refund_impact_7_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '7 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.batch_picking_to_delivery_ratio END) AS avg_batch_picking_to_delivery_ratio_7_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '7 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.tip_change_ratio END) AS avg_tip_change_ratio_7_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '7 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.batch_avg_picking_time_per_item END) AS avg_batch_avg_picking_time_per_item_7_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '7 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.delivery_time_deviation END) AS avg_delivery_time_deviation_7_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '7 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.delivery_time_ratio END) AS avg_delivery_time_ratio_7_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '7 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.successful_swipe_ratio END) AS avg_successful_swipe_ratio_7_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '7 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.declined_swipe_ratio END) AS avg_declined_swipe_ratio_7_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '7 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.found_item_ratio END) AS avg_found_item_ratio_7_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '7 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.replaced_item_ratio END) AS avg_replaced_item_ratio_7_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '7 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.driving_distance_miles END) AS avg_driving_distance_miles_7_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '7 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.delta_actual_vs_predicted_mileage END) AS avg_delta_actual_vs_predicted_mileage_7_days,

        -- Counts of each fraud_vector over the past 7 days
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '7 DAY'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'pre_checkout_unassign' THEN bd2.order_delivery_id END), 0) AS num_pre_checkout_unassign_cases_7_days,
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '7 DAY'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'time_and_distance' THEN bd2.order_delivery_id END), 0) AS num_time_and_distance_cases_7_days,
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '7 DAY'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'post_checkout_unassign' THEN bd2.order_delivery_id END), 0) AS num_post_checkout_unassign_cases_7_days,
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '7 DAY'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'overspend' THEN bd2.order_delivery_id END), 0) AS num_overspend_cases_7_days,
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '7 DAY'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'earnings_abuse_bumps' THEN bd2.order_delivery_id END), 0) AS num_earnings_abuse_bumps_cases_7_days,
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '7 DAY'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'oos' THEN bd2.order_delivery_id END), 0) AS num_oos_cases_7_days,

        -- Total number of fraud cases over the past 7 days
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '7 DAY'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector != 'non-fraud' THEN bd2.order_delivery_id END), 0) AS num_fraud_cases_7_days,

        -- Aggregated features over the past 28 days
        COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '28 DAY'
                   AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.order_delivery_id END) AS num_orders_28_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '28 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.refunded_item_ratio END) AS avg_refund_ratio_28_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '28 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.picking_time_per_item_minutes END) AS avg_picking_time_per_item_28_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '28 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.overspend_ratio END) AS avg_overspend_ratio_28_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '28 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.refund_impact END) AS avg_refund_impact_28_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '28 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.batch_picking_to_delivery_ratio END) AS avg_batch_picking_to_delivery_ratio_28_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '28 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.tip_change_ratio END) AS avg_tip_change_ratio_28_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '28 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.batch_avg_picking_time_per_item END) AS avg_batch_avg_picking_time_per_item_28_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '28 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.delivery_time_deviation END) AS avg_delivery_time_deviation_28_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '28 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.delivery_time_ratio END) AS avg_delivery_time_ratio_28_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '28 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.successful_swipe_ratio END) AS avg_successful_swipe_ratio_28_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '28 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.declined_swipe_ratio END) AS avg_declined_swipe_ratio_28_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '28 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.found_item_ratio END) AS avg_found_item_ratio_28_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '28 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.replaced_item_ratio END) AS avg_replaced_item_ratio_28_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '28 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.driving_distance_miles END) AS avg_driving_distance_miles_28_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '28 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.delta_actual_vs_predicted_mileage END) AS avg_delta_actual_vs_predicted_mileage_28_days,

        -- Counts of each fraud_vector over the past 28 days
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '28 DAY'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'pre_checkout_unassign' THEN bd2.order_delivery_id END), 0) AS num_pre_checkout_unassign_cases_28_days,
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '28 DAY'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'time_and_distance' THEN bd2.order_delivery_id END), 0) AS num_time_and_distance_cases_28_days,
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '28 DAY'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'post_checkout_unassign' THEN bd2.order_delivery_id END), 0) AS num_post_checkout_unassign_cases_28_days,
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '28 DAY'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'overspend' THEN bd2.order_delivery_id END), 0) AS num_overspend_cases_28_days,
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '28 DAY'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'earnings_abuse_bumps' THEN bd2.order_delivery_id END), 0) AS num_earnings_abuse_bumps_cases_28_days,
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '28 DAY'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'oos' THEN bd2.order_delivery_id END), 0) AS num_oos_cases_28_days,

        -- Total number of fraud cases over the past 28 days
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '28 DAY'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector != 'non-fraud' THEN bd2.order_delivery_id END), 0) AS num_fraud_cases_28_days,

        -- Aggregated features over the past 84 days
        COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '84 DAY'
                   AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.order_delivery_id END) AS num_orders_84_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '84 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.refunded_item_ratio END) AS avg_refund_ratio_84_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '84 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.picking_time_per_item_minutes END) AS avg_picking_time_per_item_84_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '84 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.overspend_ratio END) AS avg_overspend_ratio_84_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '84 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.refund_impact END) AS avg_refund_impact_84_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '84 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.batch_picking_to_delivery_ratio END) AS avg_batch_picking_to_delivery_ratio_84_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '84 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.tip_change_ratio END) AS avg_tip_change_ratio_84_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '84 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.batch_avg_picking_time_per_item END) AS avg_batch_avg_picking_time_per_item_84_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '84 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.delivery_time_deviation END) AS avg_delivery_time_deviation_84_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '84 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.delivery_time_ratio END) AS avg_delivery_time_ratio_84_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '84 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.successful_swipe_ratio END) AS avg_successful_swipe_ratio_84_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '84 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.declined_swipe_ratio END) AS avg_declined_swipe_ratio_84_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '84 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.found_item_ratio END) AS avg_found_item_ratio_84_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '84 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.replaced_item_ratio END) AS avg_replaced_item_ratio_84_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '84 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.driving_distance_miles END) AS avg_driving_distance_miles_84_days,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '84 DAY'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.delta_actual_vs_predicted_mileage END) AS avg_delta_actual_vs_predicted_mileage_84_days,

        -- Counts of each fraud_vector over the past 84 days
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '84 DAY'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'pre_checkout_unassign' THEN bd2.order_delivery_id END), 0) AS num_pre_checkout_unassign_cases_84_days,
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '84 DAY'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'time_and_distance' THEN bd2.order_delivery_id END), 0) AS num_time_and_distance_cases_84_days,
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '84 DAY'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'post_checkout_unassign' THEN bd2.order_delivery_id END), 0) AS num_post_checkout_unassign_cases_84_days,
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '84 DAY'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'overspend' THEN bd2.order_delivery_id END), 0) AS num_overspend_cases_84_days,
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '84 DAY'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'earnings_abuse_bumps' THEN bd2.order_delivery_id END), 0) AS num_earnings_abuse_bumps_cases_84_days,
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '84 DAY'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'oos' THEN bd2.order_delivery_id END), 0) AS num_oos_cases_84_days,

        -- Total number of fraud cases over the past 84 days
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '84 DAY'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector != 'non-fraud' THEN bd2.order_delivery_id END), 0) AS num_fraud_cases_84_days,

        -- Aggregated features over the past 1 year
        COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '1 YEAR'
                   AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.order_delivery_id END) AS num_orders_1_year,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '1 YEAR'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.refunded_item_ratio END) AS avg_refund_ratio_1_year,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '1 YEAR'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.picking_time_per_item_minutes END) AS avg_picking_time_per_item_1_year,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '1 YEAR'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.overspend_ratio END) AS avg_overspend_ratio_1_year,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '1 YEAR'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.refund_impact END) AS avg_refund_impact_1_year,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '1 YEAR'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.batch_picking_to_delivery_ratio END) AS avg_batch_picking_to_delivery_ratio_1_year,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '1 YEAR'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.tip_change_ratio END) AS avg_tip_change_ratio_1_year,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '1 YEAR'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.batch_avg_picking_time_per_item END) AS avg_batch_avg_picking_time_per_item_1_year,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '1 YEAR'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.delivery_time_deviation END) AS avg_delivery_time_deviation_1_year,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '1 YEAR'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.delivery_time_ratio END) AS avg_delivery_time_ratio_1_year,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '1 YEAR'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.successful_swipe_ratio END) AS avg_successful_swipe_ratio_1_year,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '1 YEAR'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.declined_swipe_ratio END) AS avg_declined_swipe_ratio_1_year,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '1 YEAR'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.found_item_ratio END) AS avg_found_item_ratio_1_year,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '1 YEAR'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.replaced_item_ratio END) AS avg_replaced_item_ratio_1_year,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '1 YEAR'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.driving_distance_miles END) AS avg_driving_distance_miles_1_year,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '1 YEAR'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.delta_actual_vs_predicted_mileage END) AS avg_delta_actual_vs_predicted_mileage_1_year,

        -- Counts of each fraud_vector over the past 1 year
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '1 YEAR'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'pre_checkout_unassign' THEN bd2.order_delivery_id END), 0) AS num_pre_checkout_unassign_cases_1_year,
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '1 YEAR'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'time_and_distance' THEN bd2.order_delivery_id END), 0) AS num_time_and_distance_cases_1_year,
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '1 YEAR'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'post_checkout_unassign' THEN bd2.order_delivery_id END), 0) AS num_post_checkout_unassign_cases_1_year,
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '1 YEAR'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'overspend' THEN bd2.order_delivery_id END), 0) AS num_overspend_cases_1_year,
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '1 YEAR'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'earnings_abuse_bumps' THEN bd2.order_delivery_id END), 0) AS num_earnings_abuse_bumps_cases_1_year,
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '1 YEAR'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'oos' THEN bd2.order_delivery_id END), 0) AS num_oos_cases_1_year,

        -- Total number of fraud cases over the past 1 year
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '1 YEAR'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector != 'non-fraud' THEN bd2.order_delivery_id END), 0) AS num_fraud_cases_1_year,

        -- Aggregated features over the past 18 months
        COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '18 MONTH'
                   AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.order_delivery_id END) AS num_orders_18_months,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '18 MONTH'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.refunded_item_ratio END) AS avg_refund_ratio_18_months,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '18 MONTH'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.picking_time_per_item_minutes END) AS avg_picking_time_per_item_18_months,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '18 MONTH'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.overspend_ratio END) AS avg_overspend_ratio_18_months,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '18 MONTH'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.refund_impact END) AS avg_refund_impact_18_months,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '18 MONTH'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.batch_picking_to_delivery_ratio END) AS avg_batch_picking_to_delivery_ratio_18_months,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '18 MONTH'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.tip_change_ratio END) AS avg_tip_change_ratio_18_months,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '18 MONTH'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.batch_avg_picking_time_per_item END) AS avg_batch_avg_picking_time_per_item_18_months,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '18 MONTH'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.delivery_time_deviation END) AS avg_delivery_time_deviation_18_months,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '18 MONTH'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.delivery_time_ratio END) AS avg_delivery_time_ratio_18_months,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '18 MONTH'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.successful_swipe_ratio END) AS avg_successful_swipe_ratio_18_months,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '18 MONTH'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.declined_swipe_ratio END) AS avg_declined_swipe_ratio_18_months,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '18 MONTH'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.found_item_ratio END) AS avg_found_item_ratio_18_months,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '18 MONTH'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.replaced_item_ratio END) AS avg_replaced_item_ratio_18_months,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '18 MONTH'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.driving_distance_miles END) AS avg_driving_distance_miles_18_months,
        AVG(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '18 MONTH'
                  AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE' THEN bd2.delta_actual_vs_predicted_mileage END) AS avg_delta_actual_vs_predicted_mileage_18_months,

        -- Counts of each fraud_vector over the past 18 months
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '18 MONTH'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'pre_checkout_unassign' THEN bd2.order_delivery_id END), 0) AS num_pre_checkout_unassign_cases_18_months,
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '18 MONTH'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'time_and_distance' THEN bd2.order_delivery_id END), 0) AS num_time_and_distance_cases_18_months,
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '18 MONTH'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'post_checkout_unassign' THEN bd2.order_delivery_id END), 0) AS num_post_checkout_unassign_cases_18_months,
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '18 MONTH'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'overspend' THEN bd2.order_delivery_id END), 0) AS num_overspend_cases_18_months,
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '18 MONTH'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'earnings_abuse_bumps' THEN bd2.order_delivery_id END), 0) AS num_earnings_abuse_bumps_cases_18_months,
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '18 MONTH'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector = 'oos' THEN bd2.order_delivery_id END), 0) AS num_oos_cases_18_months,

        -- Total number of fraud cases over the past 18 months
        COALESCE(COUNT(CASE WHEN bd2.delivery_created_date_time_utc BETWEEN bd.delivery_created_date_time_utc - INTERVAL '18 MONTH'
                            AND bd.delivery_created_date_time_utc - INTERVAL '1 MINUTE'
                            AND bd2.fraud_vector != 'non-fraud' THEN bd2.order_delivery_id END), 0) AS num_fraud_cases_18_months

    FROM base_data bd
    LEFT JOIN base_data bd2
        ON bd.shopper_id = bd2.shopper_id
        AND bd2.delivery_created_date_time_utc < bd.delivery_created_date_time_utc
    GROUP BY bd.order_delivery_id, bd.shopper_id, bd.delivery_created_date_time_utc
)

SELECT DISTINCT
    bd.*,

    -- Aggregated features over the past 7 days
    af.num_orders_7_days,
    af.avg_refund_ratio_7_days,
    af.avg_picking_time_per_item_7_days,
    af.avg_overspend_ratio_7_days,
    af.avg_refund_impact_7_days,
    af.avg_batch_picking_to_delivery_ratio_7_days,
    af.avg_tip_change_ratio_7_days,
    af.avg_batch_avg_picking_time_per_item_7_days,
    af.avg_delivery_time_deviation_7_days,
    af.avg_delivery_time_ratio_7_days,
    af.avg_successful_swipe_ratio_7_days,
    af.avg_declined_swipe_ratio_7_days,
    af.avg_found_item_ratio_7_days,
    af.avg_replaced_item_ratio_7_days,
    af.avg_driving_distance_miles_7_days,
    af.avg_delta_actual_vs_predicted_mileage_7_days,
    af.num_pre_checkout_unassign_cases_7_days,
    af.num_time_and_distance_cases_7_days,
    af.num_post_checkout_unassign_cases_7_days,
    af.num_overspend_cases_7_days,
    af.num_earnings_abuse_bumps_cases_7_days,
    af.num_oos_cases_7_days,
    af.num_fraud_cases_7_days,

    -- Aggregated features over the past 28 days
    af.num_orders_28_days,
    af.avg_refund_ratio_28_days,
    af.avg_picking_time_per_item_28_days,
    af.avg_overspend_ratio_28_days,
    af.avg_refund_impact_28_days,
    af.avg_batch_picking_to_delivery_ratio_28_days,
    af.avg_tip_change_ratio_28_days,
    af.avg_batch_avg_picking_time_per_item_28_days,
    af.avg_delivery_time_deviation_28_days,
    af.avg_delivery_time_ratio_28_days,
    af.avg_successful_swipe_ratio_28_days,
    af.avg_declined_swipe_ratio_28_days,
    af.avg_found_item_ratio_28_days,
    af.avg_replaced_item_ratio_28_days,
    af.avg_driving_distance_miles_28_days,
    af.avg_delta_actual_vs_predicted_mileage_28_days,
    af.num_pre_checkout_unassign_cases_28_days,
    af.num_time_and_distance_cases_28_days,
    af.num_post_checkout_unassign_cases_28_days,
    af.num_overspend_cases_28_days,
    af.num_earnings_abuse_bumps_cases_28_days,
    af.num_oos_cases_28_days,
    af.num_fraud_cases_28_days,

    -- Aggregated features over the past 84 days
    af.num_orders_84_days,
    af.avg_refund_ratio_84_days,
    af.avg_picking_time_per_item_84_days,
    af.avg_overspend_ratio_84_days,
    af.avg_refund_impact_84_days,
    af.avg_batch_picking_to_delivery_ratio_84_days,
    af.avg_tip_change_ratio_84_days,
    af.avg_batch_avg_picking_time_per_item_84_days,
    af.avg_delivery_time_deviation_84_days,
    af.avg_delivery_time_ratio_84_days,
    af.avg_successful_swipe_ratio_84_days,
    af.avg_declined_swipe_ratio_84_days,
    af.avg_found_item_ratio_84_days,
    af.avg_replaced_item_ratio_84_days,
    af.avg_driving_distance_miles_84_days,
    af.avg_delta_actual_vs_predicted_mileage_84_days,
    af.num_pre_checkout_unassign_cases_84_days,
    af.num_time_and_distance_cases_84_days,
    af.num_post_checkout_unassign_cases_84_days,
    af.num_overspend_cases_84_days,
    af.num_earnings_abuse_bumps_cases_84_days,
    af.num_oos_cases_84_days,
    af.num_fraud_cases_84_days,

    -- Aggregated features over the past 1 year
    af.num_orders_1_year,
    af.avg_refund_ratio_1_year,
    af.avg_picking_time_per_item_1_year,
    af.avg_overspend_ratio_1_year,
    af.avg_refund_impact_1_year,
    af.avg_batch_picking_to_delivery_ratio_1_year,
    af.avg_tip_change_ratio_1_year,
    af.avg_batch_avg_picking_time_per_item_1_year,
    af.avg_delivery_time_deviation_1_year,
    af.avg_delivery_time_ratio_1_year,
    af.avg_successful_swipe_ratio_1_year,
    af.avg_declined_swipe_ratio_1_year,
    af.avg_found_item_ratio_1_year,
    af.avg_replaced_item_ratio_1_year,
    af.avg_driving_distance_miles_1_year,
    af.avg_delta_actual_vs_predicted_mileage_1_year,
    af.num_pre_checkout_unassign_cases_1_year,
    af.num_time_and_distance_cases_1_year,
    af.num_post_checkout_unassign_cases_1_year,
    af.num_overspend_cases_1_year,
    af.num_earnings_abuse_bumps_cases_1_year,
    af.num_oos_cases_1_year,
    af.num_fraud_cases_1_year,

    -- Aggregated features over the past 18 months
    af.num_orders_18_months,
    af.avg_refund_ratio_18_months,
    af.avg_picking_time_per_item_18_months,
    af.avg_overspend_ratio_18_months,
    af.avg_refund_impact_18_months,
    af.avg_batch_picking_to_delivery_ratio_18_months,
    af.avg_tip_change_ratio_18_months,
    af.avg_batch_avg_picking_time_per_item_18_months,
    af.avg_delivery_time_deviation_18_months,
    af.avg_delivery_time_ratio_18_months,
    af.avg_successful_swipe_ratio_18_months,
    af.avg_declined_swipe_ratio_18_months,
    af.avg_found_item_ratio_18_months,
    af.avg_replaced_item_ratio_18_months,
    af.avg_driving_distance_miles_18_months,
    af.avg_delta_actual_vs_predicted_mileage_18_months,
    af.num_pre_checkout_unassign_cases_18_months,
    af.num_time_and_distance_cases_18_months,
    af.num_post_checkout_unassign_cases_18_months,
    af.num_overspend_cases_18_months,
    af.num_earnings_abuse_bumps_cases_18_months,
    af.num_oos_cases_18_months,
    af.num_fraud_cases_18_months

FROM base_data bd
LEFT JOIN aggregated_features af ON bd.order_delivery_id = af.order_delivery_id