
view: breakage_analysis {
  derived_table: {
    sql:
     --BREAKAGE QUERY BY CARRIER (UPS, USPS, FEDEX) FOR ACCOUNTING, UNUSED LABEL = FIRST SCAN DATE IS NOT NULL, REMOVE REFUNDS PURCHASED OUT OF PERIOD
      WITH query_variables AS
              (
              SELECT
              -- Date filter for whole query --> What PURCHASE DATE range are we looking to pull?
                '2023-01-01 00:00:00'::timestamp AS query_start_timestamp_filter,
                current_date::timestamp AS query_end_timestamp_filter
              )

      SELECT
          carrier_name,
          --service_level_name,
          --carrier_service_level_name,
          --entry_method,
          --user_id,
          --company_name,

          TO_CHAR(TO_DATE(purchase_date,'YYYY-MM-DD'),'YYYY-MM') as purchase_month,
          TO_DATE(purchase_date,'YYYY-MM-DD') as purchase_date,
          TO_CHAR(TO_DATE(track_first_event_date,'YYYY-MM-DD'),'YYYY-MM') as track_first_event_month,
          transaction_type,
          --CASE WHEN
          --    query IN ('purchase_refund_in_period')
          --    THEN TO_CHAR(TO_DATE(track_first_event_date,'YYYY-MM-DD'),'YYYY-MM')
          --    END AS test,
          --TO_CHAR(TO_DATE(refund_date,'YYYY-MM-DD'), 'YYYY-MM') as refund_month,

          AVG(
              CASE WHEN
                  query IN ('purchase_refund_in_period')
                  THEN COALESCE(shippo_cost,0)*-1
                  END) avg_cost_per_lbl,
          COUNT(DISTINCT
              CASE WHEN
                  query IN ('purchase_refund_in_period')
                  THEN tracking_number
                  END) no_labels_purchased,
          COUNT(DISTINCT
              CASE WHEN
                  (
                  refund_status IN ('SUCCESS')
                      AND
                          (
                          refund_date >= (select query_start_timestamp_filter from query_variables) -- '2020-07-01'
                              AND
                                  refund_date < (select query_end_timestamp_filter from query_variables) -- '2023-01-01'
                          )
                          --AND
                          --    refund_date >= purchase_date + INTERVAL '30 days'
                  )
                  THEN tracking_number
                  END) no_labels_refunded,
      --    COUNT(DISTINCT
      --        CASE WHEN
      --            (
      --            query IN ('purchase_refund_in_period')
      --                AND
      --                    (
      --                    refund_status IN ('SUCCESS')
      --                        AND
      --                            (
      --                            refund_date >= '2020-07-01'
      --                                AND
      --                                    refund_date < '2023-01-01'
      --                            )
      --                        --    AND
      --                        --        refund_date >= purchase_date + INTERVAL '30 days'
      --                    )
      --            )
      --            THEN tracking_number
      --            END) no_labels_refunded_purchased_in_period,
      --    COUNT(DISTINCT
      --        CASE WHEN
      --            (
      --            query IN ('refund_out_of_period')
      --                AND
      --                    (
      --                    refund_status IN ('SUCCESS')
      --                        AND
      --                            (
      --                            refund_date >= '2020-07-01'
      --                                AND
      --                                    refund_date < '2023-01-01'
      --                            )
      --                        --    AND
      --                        --        refund_date >= purchase_date + INTERVAL '30 days'
      --                    )
      --            )
      --            THEN tracking_number
      --            END) no_labels_refunded_purchased_out_of_period,
          COUNT(DISTINCT
              CASE WHEN
                  (
                  unused_labels_from_status IN ('unused')
                      AND
                          query IN ('purchase_refund_in_period')
                  )
                  THEN tracking_number
                  END) no_unused_labels_purchased_from_status,
          COUNT(DISTINCT
              CASE WHEN
                  (
                  refund_status IN ('SUCCESS')
                      AND
                          unused_labels_from_status IN ('unused')
                          AND
                              (
                                  refund_date >= (select query_start_timestamp_filter from query_variables) -- '2020-07-01'
                                      AND
                                          refund_date < (select query_end_timestamp_filter from query_variables) -- '2023-01-01'
                              )
                              --AND
                              --    refund_date >= purchase_date + INTERVAL '30 days'
                  )
                  THEN tracking_number
                  END) no_unused_labels_refunded_from_status,
          COUNT(DISTINCT
              CASE WHEN
                  (
                  unused_labels IN ('unused')
                      AND
                          query IN ('purchase_refund_in_period')
                  )
                  THEN tracking_number
                  END) no_unused_labels_purchased,
          COUNT(DISTINCT
              CASE WHEN
                  (
                  refund_status IN ('SUCCESS')
                      AND
                          unused_labels IN ('unused')
                          AND
                              (
                                  refund_date >= (select query_start_timestamp_filter from query_variables) -- '2020-07-01'
                                      AND
                                          refund_date < (select query_end_timestamp_filter from query_variables) -- '2023-01-01'
                              )
                              --AND
                              --    refund_date >= purchase_date + INTERVAL '30 days'
                  )
                  THEN tracking_number
                  END) no_unused_labels_refunded,
      --    COUNT(DISTINCT
      --        CASE WHEN
      --            (
      --            unused_labels IN ('unused')
      --                AND
      --                    query IN ('purchase_refund_in_period')
      --            )
      --            THEN tracking_number
      --            END) -
      --    COUNT(DISTINCT
      --        CASE WHEN
      --            (
      --            refund_status IN ('SUCCESS')
      --                AND
      --                    unused_labels IN ('unused')
      --                    AND
      --                        (
      --                            refund_date >= '2020-07-01'
      --                                AND
      --                                    refund_date < '2023-01-01'
      --                        )
      --                        --AND
      --                        --    refund_date >= purchase_date + INTERVAL '30 days'
      --            )
      --            THEN tracking_number
      --            END) no_unused_labels_remaining,
          SUM(
              CASE WHEN
                  query IN ('purchase_refund_in_period')
                  THEN COALESCE(shippo_cost,0)*-1
                  END) label_purchase_cost,
          SUM(
              CASE WHEN
                  (
                  refund_status IN ('SUCCESS')
                      AND
                          (
                          refund_date >= (select query_start_timestamp_filter from query_variables) -- '2020-07-01'
                              AND
                                  refund_date < (select query_end_timestamp_filter from query_variables) -- '2023-01-01'
                          )
                          --AND
                          --    refund_date >= purchase_date + INTERVAL '30 days'
                  )
                  THEN COALESCE(ref_shippo_cost,0)*-1
                  END) refunded_label_cost,
          SUM(
              CASE WHEN
                  (
                  query IN ('purchase_refund_in_period')
                      AND
                          unused_labels_from_status IN ('unused')
                  )
                  THEN COALESCE(shippo_cost,0)*-1
                  END) unused_label_cost_from_status,
          SUM(
              CASE WHEN
                  (
                  refund_status IN ('SUCCESS')
                      AND
                          unused_labels_from_status IN ('unused')
                          AND
                              (
                                  refund_date >= (select query_start_timestamp_filter from query_variables) -- '2020-07-01'
                                      AND
                                          refund_date < (select query_end_timestamp_filter from query_variables) -- '2023-01-01'
                              )
                              --AND
                              --    refund_date >= purchase_date + INTERVAL '30 days'
                  )
                  THEN COALESCE(ref_shippo_cost,0)*-1
                  END) unused_label_refund_cost_from_status,
          SUM(
              CASE WHEN
                  (
                  query IN ('purchase_refund_in_period')
                      AND
                          unused_labels IN ('unused')
                  )
                  THEN COALESCE(shippo_cost,0)*-1
                  END) unused_label_cost,
          SUM(
              CASE WHEN
                  (
                  refund_status IN ('SUCCESS')
                      AND
                          unused_labels IN ('unused')
                          AND
                              (
                                  refund_date >= (select query_start_timestamp_filter from query_variables) -- '2020-07-01'
                                      AND
                                          refund_date < (select query_end_timestamp_filter from query_variables) -- '2023-01-01'
                              )
                              --AND
                              --    refund_date >= purchase_date + INTERVAL '30 days'
                  )
                  THEN COALESCE(ref_shippo_cost,0)*-1
                  END) unused_label_refund_cost
                  --,
      --    SUM(
      --        CASE WHEN --unused_label_cost
      --            (
      --            query IN ('purchase_refund_in_period')
      --                AND
      --                    unused_labels IN ('unused')
      --            )
      --            THEN COALESCE(shippo_cost,0)*-1
      --            END) +
      --    SUM(
      --        CASE WHEN --unused_label_refund
      --            (
      --            refund_status IN ('SUCCESS')
      --                AND
      --                    unused_labels IN ('unused')
      --                    AND
      --                        (
      --                            refund_date >= '2020-07-01'
      --                                AND
      --                                    refund_date < '2023-01-01'
      --                        )
      --                        --AND
      --                        --    refund_date >= purchase_date + INTERVAL '30 days'
      --            )
      --            THEN COALESCE(ref_shippo_cost,0)*-1
      --            END) remaining_unused_label_cost


      FROM
      (
      SELECT DISTINCT
          'purchase_refund_in_period'                         query,
          sld.service_level_name,
          sld.carrier_service_level_name,
          lf.tracking_number,
          TO_CHAR(lf.create_date,'YYYY-MM-DD')                created_date,
          TO_CHAR(dd.full_date,'YYYY-MM-DD')                  purchase_date,
          COALESCE(ROUND(lf.postage_price_usd,2),0)           user_rate,
          COALESCE(ROUND(lf.postage_revenue_usd,2),0)         shippo_charged,
          COALESCE(ROUND(lf.invoiced_amount_usd,2),0)         inv_amount,
          COALESCE(ROUND(lf.postage_cost_usd,2),0)            shippo_cost, --actual_postage_cost,
          opcd.postal_code                                    origin_zip,
          dpcd.postal_code                                    destination_zip,
          ROUND(lf.weight_lb,1)                               parcel_weight_lb,
          COALESCE(ROUND(lf.length_mm/25.4,1),0) || ' x ' ||
          COALESCE(ROUND(lf.width_mm/25.4,1),0) || ' x ' ||
          COALESCE(ROUND(lf.height_mm/25.4,1),0)              parcel_dimension_lwh_in,
          --lf.length_mm,
          --lf.width_mm,
          --lf.height_mm,
          zd.zone_name                                        carrier_zone,
          lf.transaction_id,
          cad.carrier_name,
          ttd.transaction_type,
          ptd.parcel_type,
          tfed.full_date                                      track_first_event_date,
          tled.full_date                                      track_last_event_date,
          tded.full_date                                      track_delivery_event_date,
          tced.full_date                                      track_created_event_date,
          toded.full_date                                     track_out_for_delivery_date,
          --tf.first_event_date_dim_id,
          --tf.last_event_date_dim_id,
          tf.track_status_dim_id                              track_status_id,
          tsd.track_status_name,
          ref.transaction_type                                has_refund,
          ref.refund_status,
          ref.refund_date,
          ref.user_rate                                       ref_user_rate,
          ref.shippo_charged                                  ref_shippo_charged,
          ref.shippo_cost                                     ref_shippo_cost,
          --rsd.refund_status
          ud.user_id,
          ud.company_name,
          emd.entry_method_type                               entry_method,
          CASE WHEN
              tsd.track_status_name NOT IN
              (
              --'UNKNOWN',
              'TRANSIT',
              'DELIVERED',
              --'FAILURE',
              'RETURNED'
              --'PRE_TRANSIT'
              )
              --AND
              --    (
              --    tfed.full_date IS NULL --Track First Event DATE
              --    OR
              --        tfed.full_date = 0
              --        OR
              --          tfed.full_date IN ('2035-12-31')
              --    )
              --AND
              --    (
              --    tled.full_date IS NULL --Track Last Event DATE
              --    OR
              --        tled.full_date = 0
              --        OR
              --          tled.full_date IN ('2035-12-31')
              --    )
              --OR
                  --(
                  --tded.full_date IS NULL
                  --OR
                      --tded.full_date = 0
                  --)
              --OR
                  --(
                  --toded.full_date IS NULL
                  --OR
                      --toded.full_date = 0
                  --)
              THEN 'unused'
              END AS unused_labels_from_status,
          CASE WHEN
                  (
                  tfed.full_date IS NULL --Track First Event DATE
                  OR
                      tfed.full_date = 0
                      OR
                        tfed.full_date IN ('2035-12-31')
                  )
              THEN 'unused'
              END AS unused_labels

          --lf.invoice_id,
          --lf.postage_est_cost_usd                           est_postage_cost,
          --lf.insurance_price_usd                            insurance_fee,
          --lf.insurance_cost_usd                             insurance_cost,
          --lf.invoiced_paid_usd                              inv_paid
      FROM prod.label_fact lf

          LEFT JOIN prod.service_level_dim sld ON lf.service_level_dim_id = sld.service_level_dim_id
          LEFT JOIN prod.date_dim dd ON lf.purchase_date_dim_id = dd.date_dim_id
          LEFT JOIN prod.postal_code_dim opcd ON lf.source_zip_dim_id = opcd.postal_code_dim_id
          LEFT JOIN prod.postal_code_dim dpcd ON lf.dest_zip_dim_id = dpcd.postal_code_dim_id
          LEFT JOIN prod.zone_dim zd ON lf.zone_dim_id = zd.zone_dim_id
          LEFT JOIN prod.carrier_account_dim cad ON lf.carrier_account_dim_id = cad.carrier_account_dim_id
          LEFT JOIN prod.user_dim ud ON lf.user_dim_id = ud.user_dim_id
          LEFT JOIN prod.transaction_type_dim ttd ON lf.transaction_type_dim_id = ttd.transaction_type_dim_id
          LEFT JOIN prod.parcel_type_dim ptd ON lf.parcel_type_dim_id = ptd.parcel_type_dim_id
          LEFT JOIN prod.refund_status_dim rsd ON lf.refund_status_dim_id = rsd.refund_status_dim_id
          LEFT JOIN prod.track_fact tf ON lf.transaction_id = tf.transaction_id
          LEFT JOIN prod.track_status_dim tsd ON tf.track_status_dim_id = tsd.track_status_dim_id
          LEFT JOIN prod.date_dim tfed ON tf.first_event_date_dim_id = tfed.date_dim_id --First Event Date
          LEFT JOIN prod.date_dim tled ON tf.last_event_date_dim_id = tled.date_dim_id --Last Event Date
          LEFT JOIN prod.date_dim tded ON tf.delivery_date_dim_id = tded.date_dim_id --Delivery Event Date
          LEFT JOIN prod.date_dim tced ON tf.trackable_created_date_dim_id = tced.date_dim_id --Created Event Date
          LEFT JOIN prod.date_dim toded ON tf.out_for_delivery_date_dim_id = toded.date_dim_id --Out for Delivery Event Date
          LEFT JOIN prod.entry_method_dim emd ON lf.entry_method_dim_id = emd.entry_method_dim_id
          LEFT JOIN
              (
              SELECT DISTINCT
                  lf.tracking_number,
                  to_char(lf.create_date,'YYYY-MM-DD')                created_date,
                  to_char(dd.full_date,'YYYY-MM-DD')                  refund_date,
                  COALESCE(ROUND(lf.postage_price_usd,2),0)           user_rate,
                  COALESCE(ROUND(lf.postage_revenue_usd,2),0)         shippo_charged,
                  COALESCE(ROUND(lf.invoiced_amount_usd,2),0)         inv_amount,
                  COALESCE(ROUND(lf.postage_cost_usd,2),0)            shippo_cost, --actual_postage_cost,
                  lf.transaction_id,
                  ttd.transaction_type,
                  ptd.parcel_type,
                  ptd.is_return,
                  rsd.refund_status
                  --ud.user_id,
                  --ud.company_name,
                  --lf.invoice_id,
                  --lf.postage_est_cost_usd                           est_postage_cost,
                  --lf.insurance_price_usd                            insurance_fee,
                  --lf.insurance_cost_usd                             insurance_cost,
                  --lf.invoiced_paid_usd                              inv_paid
              FROM prod.label_fact lf
                  LEFT JOIN prod.carrier_account_dim cad ON lf.carrier_account_dim_id = cad.carrier_account_dim_id
                  LEFT JOIN prod.date_dim dd ON lf.purchase_date_dim_id = dd.date_dim_id
                  LEFT JOIN prod.user_dim ud ON lf.user_dim_id = ud.user_dim_id
                  LEFT JOIN prod.transaction_type_dim ttd ON lf.transaction_type_dim_id = ttd.transaction_type_dim_id
                  LEFT JOIN prod.parcel_type_dim ptd ON lf.parcel_type_dim_id = ptd.parcel_type_dim_id
                  LEFT JOIN prod.refund_status_dim rsd ON lf.refund_status_dim_id = rsd.refund_status_dim_id
              WHERE
                  ttd.transaction_type IN
                  (
                  --'Purchase'
                  --,
                  'Refund',
                  'Carrier Refund',
                  'Customer Refund'
                      -- ,
                  --'Surcharge',
                  --'Adjustment',
                  --'Global Address Validation',
                  --'Dummy Label',
                  --'Tracking'
                  )
                      AND
                          rsd.refund_status IN
                          (
                          --'NOT SET',
                          --'PENDING',
                          --'QUEUED',
                          'SUCCESS'
                          --,
                          --'ERROR'
                          )
              ) ref ON lf.transaction_id = ref.transaction_id
      WHERE
          (
          dd.full_date >= (select query_start_timestamp_filter from query_variables) --'2020-07-01'
              AND
                  dd.full_date < (select query_end_timestamp_filter from query_variables) --'2023-01-01'
          )
          AND
              cad.carrier_own_account_indicator IN
              (
               --'Managed 3rd Party Master Account',
               'Express Save Master Account',
               'Shippo FC Master Account',
               'Shippo Master Account'
              )
          AND
              (cad.provider_id in ('10','17','5') or cad.carrier_name in ('UPS','USPS','FedEx'))
              --(cad.provider_id in ('5') or cad.carrier_name in ('FedEx'))
              --(cad.provider_id in ('10','17') or cad.carrier_name in ('UPS','USPS'))
              --(cad.provider_id in ('10') or cad.carrier_name in ('UPS'))
              --(cad.provider_id in ('17') or cad.carrier_name in ('USPS'))
          --AND
              --sld.service_level_name IN
              --(
              --'Priority Mail',
              --'Ground Advantage'
              --)
          AND
              (
              ud.company_name NOT ILIKE ('%goshippo.com%')
                  AND
                      ud.company_name NOT ILIKE ('%Popout%')
                          AND
                              ud.company_name NOT ILIKE ('Shippo%')
              )
          AND
              ttd.transaction_type IN
              (
              'Purchase'
              --,
              --'Refund',
              --'Surcharge',
              --'Adjustment',
              --'Global Address Validation',
              --'Dummy Label',
              --'Tracking'
              )
          AND
              ptd.parcel_type IN
              (
              --'NOT SET',
              --'return',
              'outbound'
              )
          AND tsd.track_status_name IS NOT NULL
          --AND
          --    rsd.refund_status IN
          --    (
              --'NOT SET',
              --'PENDING',
              --'QUEUED',
              --'SUCCESS'--,
              --'ERROR'
          --    )
          --AND ud.company_name NOT LIKE ('gmail.com-%')
          --AND ud.company_name NOT LIKE ('gmail.com')
      )

      --WHERE
          --((
          --refund_status IN ('SUCCESS')
          --    AND
          --        refund_date >= purchase_date + INTERVAL '30 days'
          --)
          --OR
          --    (
          --    refund_status NOT IN ('SUCCESS')
          --        OR
          --            refund_status IS NULL
          --    )
          --)

          --AND user_id IN ('1715460')
          --AND tracking_number IN ('92001901755477006001781820')
      GROUP BY carrier_name, purchase_month,purchase_date, track_first_event_month,transaction_type --service_level_name, entry_method, --, refund_month --user_id, company_name --carrier_service_level_name
      --ORDER BY carrier_name, no_labels_purchased DESC
       ;;

  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: carrier_name {
    type: string
    sql: ${TABLE}.carrier_name ;;
  }

  dimension: purchase_month {
    type: string
    sql: ${TABLE}.purchase_month ;;
  }

  dimension: purchase_date {
    type: date
    sql: ${TABLE}.purchase_date ;;
  }

  dimension: track_first_event_month {
    type: string
    sql: ${TABLE}.track_first_event_month ;;
  }

  dimension: transaction_type {
    type: string
    sql: ${TABLE}.transaction_type ;;
  }
  measure: avg_cost_per_lbl {
    type: average
    sql: ${TABLE}.avg_cost_per_lbl ;;
  }

  measure: no_labels_purchased {
    type: sum
    sql: ${TABLE}.no_labels_purchased ;;
  }

  measure: no_labels_refunded {
    type: sum
    sql: ${TABLE}.no_labels_refunded ;;
  }

  measure: no_unused_labels_purchased_from_status {
    type: sum
    sql: ${TABLE}.no_unused_labels_purchased_from_status ;;
  }

  measure: no_unused_labels_refunded_from_status {
    type: sum
    sql: ${TABLE}.no_unused_labels_refunded_from_status ;;
  }

  measure: no_unused_labels_purchased {
    type: sum
    sql: ${TABLE}.no_unused_labels_purchased ;;
  }

  measure: no_unused_labels_refunded {
    type: sum
    sql: ${TABLE}.no_unused_labels_refunded ;;
  }

  measure: label_purchase_cost {
    type: sum
    sql: ${TABLE}.label_purchase_cost ;;
  }

  measure: refunded_label_cost {
    type: sum
    sql: ${TABLE}.refunded_label_cost ;;
  }

  measure: unused_label_cost_from_status {
    type: sum
    sql: ${TABLE}.unused_label_cost_from_status ;;
  }

  measure: unused_label_refund_cost_from_status {
    type: sum
    sql: ${TABLE}.unused_label_refund_cost_from_status ;;
  }

  measure: unused_label_cost {
    type: sum
    sql: ${TABLE}.unused_label_cost ;;
  }

  measure: unused_label_refund_cost {
    type: sum
    sql: ${TABLE}.unused_label_refund_cost ;;
  }

  set: detail {
    fields: [
        carrier_name,
  purchase_month,
  purchase_date,
  track_first_event_month,
  transaction_type,
  avg_cost_per_lbl,
  no_labels_purchased,
  no_labels_refunded,
  no_unused_labels_purchased_from_status,
  no_unused_labels_refunded_from_status,
  no_unused_labels_purchased,
  no_unused_labels_refunded,
  label_purchase_cost,
  refunded_label_cost,
  unused_label_cost_from_status,
  unused_label_refund_cost_from_status,
  unused_label_cost,
  unused_label_refund_cost
    ]
  }
}
