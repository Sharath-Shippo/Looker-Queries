
view: cc_fees {
  derived_table: {
    sql: SELECT
          'VIP REV SHARE' as Category,
          'CC Fees' as VIP_Revshare_Group,
          'For API Integration White Label Partners' as type,
          user_id,
          --transaction_id,
          (TO_CHAR(DATE_TRUNC('second', convert_timezone('US/Pacific', 'UTC',
          (date((DATE(date_dim.full_date ))) || ' ' || time_dim.time_of_day)::timestamp)), 'YYYY-MM-DD HH24:MI:SS')) AS invoice_ready_for_charge_datetime,
          purchase_date_mon,
          company_name,
          carrier_name,
          carrier_own_account_indicator,
          carrier_service_level_name,
          user_payment_method,
          invoice_id,
          transaction_type,
          platform_name,
          --registration_source_mapped,
          --store_platform_name  AS store_platform_name,

          --
          --plan_name,
          --entry_method_type,

          --count(DISTINCT transaction_id) Transactions,
          sum(quantity) as Labels_count,
          --count(transaction_id) count,
          --sum("user_rate") rate,
          sum(-1*actual_postage_cost) postage_cost,
          sum(user_rate) as user_rate

      FROM
          (
              WITH query_variables AS
                       (SELECT
                            -- Date filter for whole query --> What dates range are we looking to pull?
                            '20230101' AS query_start_filter,
                            '20240301' AS query_end_filter,
                            -- BELOW IS TO TOGGLE WHETHER OR NOT ESI/ CEC ACCOUNTS ARE INCLUDED/ EXCLUDED FROM QUERY
                            --'EXCLUDE ESI, CEC'
                            'INCLUDE ESI, NO CEC'
                                --'INCLUDE CEC, NO ESI'
                                --'INCLUDE ESI, CEC'
                                --'CEC ONLY'
                                --'ESI ONLY'
                                --'PARTIAL CEC ONLY' -- THIS LOOKS AT PROD TRANSACTIONS WHERE TRANSACTIONS ARE ASSOCIATED WITH SHIPPO MASTER AND
                                -- ESI MASTER, BUT THE master_carrier_ext_account_id CONTAINS CPP OR CEC DOES
                                -- NO COMPARABLE USPS DATA SET, THIS WILL COMPARE TO ALL OF CEC USPS
                                --'CEC INTERNATIONAL ONLY' -- LOOKS FOR INTERNATIONAL TRANSACTIONS IN 'Managed 3rd Party Master Account'
                                -- AND 'carrier own account'
                                --'EXCLUDE CEC NATIONAL' -- THIS SHOULD BE THE IDEAL DATA SET INCLUDING BOTH SHIPPO MASTER ACCOUNTS, ESI,
                                -- AND CEC INTERNATIONAL, IT CAN'T BE DIRECTLY MATCHED TO USPS BECAUSE CEC NATIONAL WILL
                                -- MATCH TO TRANSACTIONS SHOWING AS SHIPPO MASTER IN PROD
                                       AS include_accounts,
                            -- 'INDITEX_ZARA' -- THIS LOOKS AT WHETHER THE CUSTOMER IS ZARA USER ID: 1206048 | USPS MASTER: 1000043690
                            'NOT_INDITEX_ZARA'
                                       AS zara
                       ),
                    user_dim AS (SELECT
              user_table.*,
              platform_table.payment_method platform_payment_method
            FROM prod.user_dim_vw user_table
            LEFT JOIN (
              SELECT distinct platform_id, payment_method
              FROM prod.user_dim
              WHERE payment_method != 'NOT SET' and platform_id != 1 AND user_id <> 2992790
            ) platform_table
            ON user_table.platform_id = platform_table.platform_id
            WHERE user_dim_id <> -1)
              SELECT
                  -- SHOW QUERY PARAMETERS FROM WITH STATEMENT
                  lf.transaction_id,
                  ud.platform_name,
                  ud.registration_source_mapped  AS registration_source_mapped,
                  ud.registration_source_commission,
                  ud.company_name  AS company_name,
                  ud.user_id  AS user_id,
                  user_billing_plan_dim.plan_name  AS plan_name,
                  sld.carrier_service_level_name  AS carrier_service_level_name,
                  ud.payment_method,
                  coalesce(platform_payment_method,ud.payment_method)  AS user_payment_method,
                  SD.store_platform_name  AS store_platform_name,
      -- Amount Fields
                  lf.postage_est_cost_usd                 est_postage_cost,
                  lf.postage_cost_usd                     actual_postage_cost,
                  -- Postage price usd = rate.amount (user rate)
                  lf.postage_price_usd                    user_rate,
                  -- Missin invoice charge, invoice refund (replicatd with case statements below)
                  -- Missing insurance amount
                  -- Per Calvin 20220401, the fee merchant pays for insurance
                  lf.insurance_price_usd                  insurance_fee,
                  -- Per Calvin, the cost Shippo pays insurer
                  lf.insurance_cost_usd                   insurance_cost,
                  -- Missing all recon table fields (only in prod)
                  -- <<REDSHIFT SPECIFIC FIELDS BELOW>>
                  lf.invoiced_amount_usd                  inv_amount,
                  lf.invoiced_paid_usd                    inv_paid,
                  lf.postage_revenue_usd,
                  lf.label_fee_usd,
                  lf.insurance_price_usd,
                  lf.address_validation_revenue_usd,
                  lf.license_fee_usd,
                  lf.insurance_cost_usd,
                  lf.carrier_partner_rev_share_usd,
                  lf.carrier_referral_fee_usd,
                  lf.user_rev_share_usd,
                  lf.quantity,
                  lf.payment_provider_fee,
                  lf.purchase_date_dim_id,
      -- Attribute Fields
                  zd.zone_name,
                  -- Missing txn object state
                  -- Missing txn object status
                  lf.tracking_number,
                  emd.entry_method_type,
                  -- Missing scan form id
                  ptd.is_return,
                  -- Missing return of id
                  -- Missing submission id
                  mtd.manifest_type,
                  -- Missing ship submission type
                  sld.service_level_name,
                  -- Note cad.provider_id is broken per Calvin must use cd. only
                  cd.provider_id,
                  cd.carrier_name,
                  cad.master_carrier_account_id,
                  cad.carrier_account_id,
                  cad.master_carrier_ext_account_id,
                  rsd.refund_status                       shippo_refund,
                  crsd.carrier_refund_status,
                  frmaddr.postal_code                     origination_zip,
                  -- Missing return address
                  toaddr.postal_code                      destination_zip,
                  -- <<REDSHIFT SPECIFIC FIELDS BELOW>>
                  cad.carrier_own_account_indicator,
                  --ud.company_name,
                  ptd.parcel_type,
                  ttd.transaction_type,
                  rtd.refund_type,
                  lf.orig_transaction_id,
                  lf.invoice_id,
                  lf.invoice_ready_for_charge_date_dim_id,
                  lf.invoice_ready_for_charge_time_dim_id,
                  ----lf.total_actual_label_cost_usd,

      -- Transaction Type (need to determine inv chrg/ refund)
                  CASE
                      WHEN (rsd.refund_status IS NOT NULL OR ttd.transaction_type = 'Refund') AND ptd.parcel_type = 'return' THEN 'return/refund'
                      WHEN (rsd.refund_status IS NOT NULL OR ttd.transaction_type = 'Refund') THEN 'outbound/refund'
                      WHEN ptd.parcel_type = 'return' THEN 'return'
                      ELSE 'outbound'
                      END                              AS cust_transaction_type,

      -- Determine 'Subscription w/o label' or show Transaction Type
                  CASE
                      WHEN ttd.transaction_type = 'Dummy Label' THEN 'Subscription w/o Label'
                      ELSE ttd.transaction_type
                      END                              AS trx_type,


      -- Date fields
                  to_char(pdd.full_date, 'YYYY-MM-DD') AS purchase_date,
                  to_char(pdd.full_date, 'MON-YY')     AS purchase_date_mon


      -- Main LABEL FACT (LF) TABLE
              FROM prod.label_fact lf

      -- JOINS
                       LEFT JOIN prod.carrier_account_dim cad ON lf.carrier_account_dim_id = cad.carrier_account_dim_id
                       LEFT JOIN prod.carrier_dim cd ON lf.carrier_dim_id = cd.carrier_dim_id
                       LEFT JOIN prod.parcel_type_dim ptd ON lf.parcel_type_dim_id = ptd.parcel_type_dim_id
                       LEFT JOIN prod.date_dim pdd ON lf.purchase_date_dim_id = pdd.date_dim_id
                       LEFT JOIN prod.service_level_dim sld ON lf.service_level_dim_id = sld.service_level_dim_id
                       LEFT JOIN user_dim ud ON lf.user_dim_id = ud.user_dim_id
                       LEFT JOIN prod.transaction_type_dim ttd ON lf.transaction_type_dim_id = ttd.transaction_type_dim_id
                       LEFT JOIN prod.zone_dim zd ON lf.zone_dim_id = zd.zone_dim_id
                       LEFT JOIN prod.entry_method_dim emd ON lf.entry_method_dim_id = emd.entry_method_dim_id
                       LEFT JOIN prod.manifest_type_dim mtd ON lf.manifest_type_dim_id = mtd.manifest_type_dim_id
                       LEFT JOIN prod.carrier_refund_status_dim crsd ON lf.carrier_refund_status_dim_id = crsd.carrier_refund_status_dim_id
                       LEFT JOIN prod.refund_type_dim rtd ON lf.refund_type_dim_id = rtd.refund_type_dim_id
                       LEFT JOIN prod.refund_status_dim rsd ON lf.refund_status_dim_id = rsd.refund_status_dim_id
                       LEFT JOIN prod.postal_code_dim frmaddr ON lf.source_zip_dim_id = frmaddr.postal_code_dim_id
                       LEFT JOIN prod.postal_code_dim toaddr ON lf.dest_zip_dim_id = toaddr.postal_code_dim_id
                       LEFT JOIN prod.user_billing_plan_dim  AS user_billing_plan_dim ON lf.user_billing_plan_dim_id = user_billing_plan_dim.user_billing_plan_dim_id
                       LEFT JOIN prod.store_platform_dim  AS SD ON lf.store_platform_dim_id = SD.store_platform_dim_id
      --               LEFT JOIN master_accounts ma ON cad.master_carrier_account_id = ma.account

              WHERE (lf.purchase_date_dim_id >= --'20220101'
                     (select query_start_filter from query_variables)
                  AND lf.purchase_date_dim_id <= --'20230101'
                      (select query_end_filter from query_variables))
                AND ((( ud.company_name  ) NOT ILIKE  '%goshippo.com%') AND (( ud.company_name  ) NOT ILIKE  '%Popout%') AND (( ud.company_name  ) NOT ILIKE  'Shippo%') OR (ud.company_name ) IS NULL)
              ORDER BY lf.transaction_id
      --;
          )A
              INNER JOIN prod.date_dim  AS date_dim ON A.invoice_ready_for_charge_date_dim_id = date_dim.date_dim_id
              INNER JOIN prod.time_dim  AS time_dim ON A.invoice_ready_for_charge_time_dim_id = time_dim.time_dim_id
      where    (user_id) IN ('66839', '330296', '754014', '784211', '1412312')
        --and transaction_type in ('Purchase','Refund')
      GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
      union
      SELECT
          'VIP REV SHARE' as Category,
          'CC Fees' as VIP_Revshare_Group,
          'For Platform Partners' as type,
          user_id,
          --transaction_id,
          (TO_CHAR(DATE_TRUNC('second', convert_timezone('US/Pacific', 'UTC',
           (date((DATE(date_dim.full_date ))) || ' ' || time_dim.time_of_day)::timestamp)), 'YYYY-MM-DD HH24:MI:SS')) AS invoice_ready_for_charge_datetime,
          purchase_date_mon,
          company_name,
          carrier_name,
          carrier_own_account_indicator,
          carrier_service_level_name,
          user_payment_method,
          invoice_id,
          transaction_type,
          platform_name,
          --registration_source_mapped,
          --store_platform_name  AS store_platform_name,

          --
          --plan_name,
          --entry_method_type,

          --count(DISTINCT transaction_id) Transactions,
          sum(quantity) as Labels_count,
          --count(transaction_id) count,
          --sum("user_rate") rate,
          sum(-1*actual_postage_cost) postage_cost,
          sum(user_rate) as user_rate

      FROM
          (
              WITH query_variables AS
                       (SELECT
                            -- Date filter for whole query --> What dates range are we looking to pull?
                            '20230101' AS query_start_filter,
                            '20240301' AS query_end_filter,
                            -- BELOW IS TO TOGGLE WHETHER OR NOT ESI/ CEC ACCOUNTS ARE INCLUDED/ EXCLUDED FROM QUERY
                            --'EXCLUDE ESI, CEC'
                            'INCLUDE ESI, NO CEC'
                                --'INCLUDE CEC, NO ESI'
                                --'INCLUDE ESI, CEC'
                                --'CEC ONLY'
                                --'ESI ONLY'
                                --'PARTIAL CEC ONLY' -- THIS LOOKS AT PROD TRANSACTIONS WHERE TRANSACTIONS ARE ASSOCIATED WITH SHIPPO MASTER AND
                                -- ESI MASTER, BUT THE master_carrier_ext_account_id CONTAINS CPP OR CEC DOES
                                -- NO COMPARABLE USPS DATA SET, THIS WILL COMPARE TO ALL OF CEC USPS
                                --'CEC INTERNATIONAL ONLY' -- LOOKS FOR INTERNATIONAL TRANSACTIONS IN 'Managed 3rd Party Master Account'
                                -- AND 'carrier own account'
                                --'EXCLUDE CEC NATIONAL' -- THIS SHOULD BE THE IDEAL DATA SET INCLUDING BOTH SHIPPO MASTER ACCOUNTS, ESI,
                                -- AND CEC INTERNATIONAL, IT CAN'T BE DIRECTLY MATCHED TO USPS BECAUSE CEC NATIONAL WILL
                                -- MATCH TO TRANSACTIONS SHOWING AS SHIPPO MASTER IN PROD
                                       AS include_accounts,
                            -- 'INDITEX_ZARA' -- THIS LOOKS AT WHETHER THE CUSTOMER IS ZARA USER ID: 1206048 | USPS MASTER: 1000043690
                            'NOT_INDITEX_ZARA'
                                       AS zara
                       ),
                       user_dim AS (SELECT
              user_table.*,
              platform_table.payment_method platform_payment_method
            FROM prod.user_dim_vw user_table
            LEFT JOIN (
              SELECT distinct platform_id, payment_method
              FROM prod.user_dim
              WHERE payment_method != 'NOT SET' and platform_id != 1 AND user_id <> 2992790
            ) platform_table
            ON user_table.platform_id = platform_table.platform_id
            WHERE user_dim_id <> -1)
              SELECT
                  -- SHOW QUERY PARAMETERS FROM WITH STATEMENT
                  lf.transaction_id,
                  ud.platform_name,
                  ud.registration_source_mapped  AS registration_source_mapped,
                  ud.registration_source_commission,
                  ud.company_name  AS company_name,
                  ud.user_id  AS user_id,
                  user_billing_plan_dim.plan_name  AS plan_name,
                  sld.carrier_service_level_name  AS carrier_service_level_name,
                  ud.payment_method,
                  coalesce(platform_payment_method,ud.payment_method)  AS user_payment_method,
                  SD.store_platform_name  AS store_platform_name,
      -- Amount Fields
                  lf.postage_est_cost_usd                 est_postage_cost,
                  lf.postage_cost_usd                     actual_postage_cost,
                  -- Postage price usd = rate.amount (user rate)
                  lf.postage_price_usd                    user_rate,
                  -- Missin invoice charge, invoice refund (replicatd with case statements below)
                  -- Missing insurance amount
                  -- Per Calvin 20220401, the fee merchant pays for insurance
                  lf.insurance_price_usd                  insurance_fee,
                  -- Per Calvin, the cost Shippo pays insurer
                  lf.insurance_cost_usd                   insurance_cost,
                  -- Missing all recon table fields (only in prod)
                  -- <<REDSHIFT SPECIFIC FIELDS BELOW>>
                  lf.invoiced_amount_usd                  inv_amount,
                  lf.invoiced_paid_usd                    inv_paid,
                  lf.postage_revenue_usd,
                  lf.label_fee_usd,
                  lf.insurance_price_usd,
                  lf.address_validation_revenue_usd,
                  lf.license_fee_usd,
                  lf.insurance_cost_usd,
                  lf.carrier_partner_rev_share_usd,
                  lf.carrier_referral_fee_usd,
                  lf.user_rev_share_usd,
                  lf.quantity,
                  lf.payment_provider_fee,
                  lf.purchase_date_dim_id,
      -- Attribute Fields
                  zd.zone_name,
                  -- Missing txn object state
                  -- Missing txn object status
                  lf.tracking_number,
                  emd.entry_method_type,
                  -- Missing scan form id
                  ptd.is_return,
                  -- Missing return of id
                  -- Missing submission id
                  mtd.manifest_type,
                  -- Missing ship submission type
                  sld.service_level_name,
                  -- Note cad.provider_id is broken per Calvin must use cd. only
                  cd.provider_id,
                  cd.carrier_name,
                  cad.master_carrier_account_id,
                  cad.carrier_account_id,
                  cad.master_carrier_ext_account_id,
                  rsd.refund_status                       shippo_refund,
                  crsd.carrier_refund_status,
                  frmaddr.postal_code                     origination_zip,
                  -- Missing return address
                  toaddr.postal_code                      destination_zip,
                  -- <<REDSHIFT SPECIFIC FIELDS BELOW>>
                  cad.carrier_own_account_indicator,
                  --ud.company_name,
                  ptd.parcel_type,
                  ttd.transaction_type,
                  rtd.refund_type,
                  lf.orig_transaction_id,
                  lf.invoice_id,
                  lf.invoice_ready_for_charge_date_dim_id,
                  lf.invoice_ready_for_charge_time_dim_id,
                  ----lf.total_actual_label_cost_usd,

      -- Transaction Type (need to determine inv chrg/ refund)
                  CASE
                      WHEN (rsd.refund_status IS NOT NULL OR ttd.transaction_type = 'Refund') AND ptd.parcel_type = 'return' THEN 'return/refund'
                      WHEN (rsd.refund_status IS NOT NULL OR ttd.transaction_type = 'Refund') THEN 'outbound/refund'
                      WHEN ptd.parcel_type = 'return' THEN 'return'
                      ELSE 'outbound'
                      END                              AS cust_transaction_type,

      -- Determine 'Subscription w/o label' or show Transaction Type
                  CASE
                      WHEN ttd.transaction_type = 'Dummy Label' THEN 'Subscription w/o Label'
                      ELSE ttd.transaction_type
                      END                              AS trx_type,


      -- Date fields
                  to_char(pdd.full_date, 'YYYY-MM-DD') AS purchase_date,
                  to_char(pdd.full_date, 'MON-YY')     AS purchase_date_mon


      -- Main LABEL FACT (LF) TABLE
              FROM prod.label_fact lf

      -- JOINS
                       LEFT JOIN prod.carrier_account_dim cad ON lf.carrier_account_dim_id = cad.carrier_account_dim_id
                       LEFT JOIN prod.carrier_dim cd ON lf.carrier_dim_id = cd.carrier_dim_id
                       LEFT JOIN prod.parcel_type_dim ptd ON lf.parcel_type_dim_id = ptd.parcel_type_dim_id
                       LEFT JOIN prod.date_dim pdd ON lf.purchase_date_dim_id = pdd.date_dim_id
                       LEFT JOIN prod.service_level_dim sld ON lf.service_level_dim_id = sld.service_level_dim_id
                       LEFT JOIN user_dim ud ON lf.user_dim_id = ud.user_dim_id
                       LEFT JOIN prod.transaction_type_dim ttd ON lf.transaction_type_dim_id = ttd.transaction_type_dim_id
                       LEFT JOIN prod.zone_dim zd ON lf.zone_dim_id = zd.zone_dim_id
                       LEFT JOIN prod.entry_method_dim emd ON lf.entry_method_dim_id = emd.entry_method_dim_id
                       LEFT JOIN prod.manifest_type_dim mtd ON lf.manifest_type_dim_id = mtd.manifest_type_dim_id
                       LEFT JOIN prod.carrier_refund_status_dim crsd ON lf.carrier_refund_status_dim_id = crsd.carrier_refund_status_dim_id
                       LEFT JOIN prod.refund_type_dim rtd ON lf.refund_type_dim_id = rtd.refund_type_dim_id
                       LEFT JOIN prod.refund_status_dim rsd ON lf.refund_status_dim_id = rsd.refund_status_dim_id
                       LEFT JOIN prod.postal_code_dim frmaddr ON lf.source_zip_dim_id = frmaddr.postal_code_dim_id
                       LEFT JOIN prod.postal_code_dim toaddr ON lf.dest_zip_dim_id = toaddr.postal_code_dim_id
                       INNER JOIN prod.user_billing_plan_dim  AS user_billing_plan_dim ON lf.user_billing_plan_dim_id = user_billing_plan_dim.user_billing_plan_dim_id
                       INNER JOIN prod.store_platform_dim  AS SD ON lf.store_platform_dim_id = SD.store_platform_dim_id
      --               LEFT JOIN master_accounts ma ON cad.master_carrier_account_id = ma.account

              WHERE (lf.purchase_date_dim_id >= --'20220101'
                     (select query_start_filter from query_variables)
                  AND lf.purchase_date_dim_id <= --'20230101'
                      (select query_end_filter from query_variables))
                AND ((( ud.company_name  ) NOT ILIKE  '%goshippo.com%') AND (( ud.company_name  ) NOT ILIKE  '%Popout%') AND (( ud.company_name  ) NOT ILIKE  'Shippo%') OR (ud.company_name ) IS NULL)
              ORDER BY lf.transaction_id
      --;
          )A
              INNER JOIN prod.date_dim  AS date_dim ON A.invoice_ready_for_charge_date_dim_id = date_dim.date_dim_id
              INNER JOIN prod.time_dim  AS time_dim ON A.invoice_ready_for_charge_time_dim_id = time_dim.time_dim_id
      where  user_payment_method = 'CREDIT_CARD'
        AND (platform_name) IN ('Cart.com Fulfillment', 'Cart.com Storefront Production', 'Loop', 'Popshop', 'Whatnot')
        --and transaction_type in ('Purchase','Refund')
      GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: category {
    type: string
    sql: ${TABLE}.category ;;
  }

  dimension: vip_revshare_group {
    type: string
    sql: ${TABLE}.vip_revshare_group ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  dimension: user_id {
    type: number
    sql: ${TABLE}.user_id ;;
  }

  dimension: invoice_ready_for_charge_datetime {
    type: string
    sql: ${TABLE}.invoice_ready_for_charge_datetime ;;
  }

  dimension: purchase_date_mon {
    type: string
    sql: ${TABLE}.purchase_date_mon ;;
  }

  dimension: company_name {
    type: string
    sql: ${TABLE}.company_name ;;
  }

  dimension: carrier_name {
    type: string
    sql: ${TABLE}.carrier_name ;;
  }

  dimension: carrier_own_account_indicator {
    type: string
    sql: ${TABLE}.carrier_own_account_indicator ;;
  }

  dimension: carrier_service_level_name {
    type: string
    sql: ${TABLE}.carrier_service_level_name ;;
  }

  dimension: user_payment_method {
    type: string
    sql: ${TABLE}.user_payment_method ;;
  }

  dimension: invoice_id {
    type: number
    sql: ${TABLE}.invoice_id ;;
  }

  dimension: transaction_type {
    type: string
    sql: ${TABLE}.transaction_type ;;
  }

  dimension: platform_name {
    type: string
    sql: ${TABLE}.platform_name ;;
  }

  measure: labels_count {
    type: sum
    sql: ${TABLE}.labels_count ;;
  }

  measure: postage_cost {
    type: sum
    sql: ${TABLE}.postage_cost ;;
  }

  measure: user_rate {
    type: sum
    sql: ${TABLE}.user_rate ;;
  }

  set: detail {
    fields: [
        category,
  vip_revshare_group,
  type,
  user_id,
  invoice_ready_for_charge_datetime,
  purchase_date_mon,
  company_name,
  carrier_name,
  carrier_own_account_indicator,
  carrier_service_level_name,
  user_payment_method,
  invoice_id,
  transaction_type,
  labels_count,
  postage_cost,
  user_rate
    ]
  }
}
