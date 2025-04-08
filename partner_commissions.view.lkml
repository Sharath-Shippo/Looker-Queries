
view: partner_commissions {
  derived_table: {
    sql: SELECT
          'Partner Commissions' as Category,
           case when registration_source_mapped in ('1440','boxstorm','fishbowl','freestyle','integrasoft','orangemarmalade','spoton','veeqo','zibbet') then 'RegSource'
         when registration_source_mapped in ('square') then 'WeeblySquare'
        when registration_source_mapped in ('loupe tech inc.') then 'loupe tech inc.'
         when registration_source_mapped in ('westerncomputer','commerce7') then 'Other'
         when registration_source_mapped in ('snapfulfil') then 'snapfulfil'
         when registration_source_mapped in ('woocommerce','godaddy','bigcommerce') then 'StorePlatforms'
         when registration_source_mapped in ('skulabs') then 'API'
         when store_platform_name in ('wix') and (entry_method_type) <> 'WIX-ELEMENTS' then 'Wix Platform'
         when entry_method_type in ('WIX-ELEMENTS') then 'Wix Elements' else 'unknown' end as Partner_Commission_Group,
          --transaction_id,
          --user_id,
          entry_method_type,
          company_name,
          store_platform_name  AS store_platform_name,
          registration_source_mapped,
          plan_name,
          purchase_date_mon,
          carrier_name,
          carrier_own_account_indicator,
breakage_indicator,
          user_payment_method,
          --platform_name,

          --carrier_service_level_name,
          transaction_type,
          sum(quantity) as Labels_count,
          COALESCE(SUM((CASE
                            WHEN  carrier_own_account_indicator   IN ('Managed 3rd Party Master Account') THEN 0 --CeC
                            WHEN  carrier_own_account_indicator   NOT IN ('Customer Own Account','PARTNER_BYOA','Managed 3rd Party Master Account','Marketplace Account')
                                AND  carrier_account_id   != 1524
                                THEN user_rate+COALESCE((CASE WHEN  provider_id   = 33 THEN 0.9 * DECODE(est_postage_cost, 0, actual_postage_cost, est_postage_cost) ELSE  DECODE(est_postage_cost, 0, actual_postage_cost, est_postage_cost) END), 0)
                            ELSE
                                0
              END) ), 0) AS label_markup,
          COALESCE(SUM(insurance_price_usd + COALESCE(insurance_cost_usd,0) ), 0) AS insurance_markup,
          COALESCE(SUM(label_fee_usd), 0) AS total_label_fee_usd,
          COALESCE(SUM(CASE WHEN NOT (carrier_account_id  = 1524) THEN carrier_partner_rev_share_usd  ELSE NULL END), 0) AS carrier_partner_rev_share_usd,
          COALESCE(SUM(CASE WHEN purchase_date_dim_id >=20180401 THEN
                                payment_provider_fee
                            WHEN payment_method='CREDIT_CARD' THEN
                                    -0.02 *(postage_revenue_usd + label_fee_usd + insurance_price_usd)
                            WHEN payment_method='P2P' THEN
                                    -0.015 *(postage_revenue_usd + label_fee_usd + insurance_price_usd)
                            ELSE 0 END
                       ), 0) AS total_payment_provider_fee_usd,
          COALESCE(SUM(carrier_referral_fee_usd ), 0) AS carrier_referral_fee_usd,
          COALESCE(SUM(user_rate),0) as total_postage_price_usd,
          COALESCE(SUM((CASE
                            WHEN  carrier_own_account_indicator   IN ('Managed 3rd Party Master Account') THEN NVL(user_rev_share_usd,0) --CeC
                            WHEN  carrier_own_account_indicator   NOT IN ('Customer Own Account','PARTNER_BYOA','Managed 3rd Party Master Account','Marketplace Account')
                                AND  carrier_account_id   != 1524
                                THEN user_rate
                                         + DECODE(est_postage_cost, 0, actual_postage_cost, est_postage_cost) + user_rev_share_usd
                                - (CASE WHEN  provider_id   = 33 THEN 0.1 * DECODE(est_postage_cost, 0, actual_postage_cost, est_postage_cost) ELSE 0 END)
                            ELSE 0
              END)
              + label_fee_usd
              +insurance_price_usd
              + address_validation_revenue_usd
              +license_fee_usd
              + insurance_cost_usd
              + carrier_partner_rev_share_usd
              + carrier_referral_fee_usd
                       --       + label_fact_fin.payment_provider_fee
                       ), 0) AS net_revenue,

          --count(DISTINCT transaction_id) Transactions,

          --count(transaction_id) count,
          --sum("user_rate") rate,
          sum(actual_postage_cost) postage_cost,
          COALESCE(SUM(license_fee_usd), 0) AS "Subscription_Revenue_USD",
          COALESCE(SUM(user_rev_share_usd), 0) AS "user_rev_share_usd",
          COALESCE(SUM(total_actual_label_cost_usd),0) as total_actual_label_cost_usd


      FROM
          (
              WITH query_variables AS
                       (SELECT
                            -- Date filter for whole query --> What dates range are we looking to pull?
                            '20230801' AS query_start_filter,
                            to_char(cast(current_date as date),'YYYYMMDD') AS query_end_filter,
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
                  lf.breakage_indicator,
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
                  COALESCE((CASE WHEN ( CASE
                                               WHEN ttd.transaction_type = 'Dummy Label' THEN 'Subscription w/o Label'
                                               ELSE ttd.transaction_type
                      END )='Surcharge' AND  cd.carrier_name  ='USPS' and  ptd.parcel_type  ='return' THEN 0 ELSE lf.postage_cost_usd END), 0) AS "total_actual_label_cost_usd",

      -- Transaction Type (need to determine inv chrg/ refund)
                  CASE
                      WHEN (rsd.refund_status IS NOT NULL OR ttd.transaction_type = 'Refund'OR ttd.transaction_type = 'Carrier Refund' OR ttd.transaction_type = 'Customer Refund') AND ptd.parcel_type = 'return' THEN 'return/refund'
                      WHEN (rsd.refund_status IS NOT NULL OR ttd.transaction_type = 'Refund'OR ttd.transaction_type = 'Carrier Refund' OR ttd.transaction_type = 'Customer Refund') THEN 'outbound/refund'
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
          )
      --carrier_own_account_indicator<>'Managed 3rd Party Master Account'
      where registration_source_mapped in ('fishbowl','freestyle','boxstorm','orangemarmalade','zibbet','veeqo','integrasoft','shuup','spoton','1440','square','westerncomputer','commerce7','loupe tech inc.')
        --and transaction_type in ('Purchase','Refund')
      GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13

      union
      --registration_source_mapped in ('snapfulfil')
      -- and user_id not in ('117603','186650')
      SELECT
          'Partner Commissions' as Category,
            case when registration_source_mapped in ('1440','boxstorm','fishbowl','freestyle','integrasoft','orangemarmalade','spoton','veeqo','zibbet') then 'RegSource'
         when registration_source_mapped in ('square') then 'WeeblySquare'
        when registration_source_mapped in ('loupe tech inc.') then 'loupe tech inc.'
         when registration_source_mapped in ('westerncomputer','commerce7') then 'Other'
         when registration_source_mapped in ('snapfulfil') then 'snapfulfil'
         when registration_source_mapped in ('woocommerce','godaddy','bigcommerce') then 'StorePlatforms'
         when registration_source_mapped in ('skulabs') then 'API'
         when store_platform_name in ('wix') and (entry_method_type) <> 'WIX-ELEMENTS' then 'Wix Platform'
         when entry_method_type in ('WIX-ELEMENTS') then 'Wix Elements' else 'unknown' end as Partner_Commission_Group,
          --transaction_id,
          --user_id,
          entry_method_type,
          company_name,
          store_platform_name  AS store_platform_name,
          registration_source_mapped,
          plan_name,
          purchase_date_mon,
          carrier_name,
          carrier_own_account_indicator,
breakage_indicator,
          user_payment_method,
          --platform_name,

          --carrier_service_level_name,
          transaction_type,
          sum(quantity) as Labels_count,
          COALESCE(SUM((CASE
                            WHEN  carrier_own_account_indicator   IN ('Managed 3rd Party Master Account') THEN 0 --CeC
                            WHEN  carrier_own_account_indicator   NOT IN ('Customer Own Account','PARTNER_BYOA','Managed 3rd Party Master Account','Marketplace Account')
                                AND  carrier_account_id   != 1524
                                THEN user_rate+COALESCE((CASE WHEN  provider_id   = 33 THEN 0.9 * DECODE(est_postage_cost, 0, actual_postage_cost, est_postage_cost) ELSE  DECODE(est_postage_cost, 0, actual_postage_cost, est_postage_cost) END), 0)
                            ELSE
                                0
              END) ), 0) AS label_markup,
          COALESCE(SUM(insurance_price_usd + COALESCE(insurance_cost_usd,0) ), 0) AS insurance_markup,
          COALESCE(SUM(label_fee_usd), 0) AS total_label_fee_usd,
          COALESCE(SUM(CASE WHEN NOT (carrier_account_id  = 1524) THEN carrier_partner_rev_share_usd  ELSE NULL END), 0) AS carrier_partner_rev_share_usd,
          COALESCE(SUM(CASE WHEN purchase_date_dim_id >=20180401 THEN
                                payment_provider_fee
                            WHEN payment_method='CREDIT_CARD' THEN
                                    -0.02 *(postage_revenue_usd + label_fee_usd + insurance_price_usd)
                            WHEN payment_method='P2P' THEN
                                    -0.015 *(postage_revenue_usd + label_fee_usd + insurance_price_usd)
                            ELSE 0 END
                       ), 0) AS total_payment_provider_fee_usd,
          COALESCE(SUM(carrier_referral_fee_usd ), 0) AS carrier_referral_fee_usd,
          COALESCE(SUM(user_rate),0) as total_postage_price_usd,
          COALESCE(SUM((CASE
                            WHEN  carrier_own_account_indicator   IN ('Managed 3rd Party Master Account') THEN NVL(user_rev_share_usd,0) --CeC
                            WHEN  carrier_own_account_indicator   NOT IN ('Customer Own Account','PARTNER_BYOA','Managed 3rd Party Master Account','Marketplace Account')
                                AND  carrier_account_id   != 1524
                                THEN user_rate
                                         + DECODE(est_postage_cost, 0, actual_postage_cost, est_postage_cost) + user_rev_share_usd
                                - (CASE WHEN  provider_id   = 33 THEN 0.1 * DECODE(est_postage_cost, 0, actual_postage_cost, est_postage_cost) ELSE 0 END)
                            ELSE 0
              END)
              + label_fee_usd
              +insurance_price_usd
              + address_validation_revenue_usd
              +license_fee_usd
              + insurance_cost_usd
              + carrier_partner_rev_share_usd
              + carrier_referral_fee_usd
                       --       + label_fact_fin.payment_provider_fee
                       ), 0) AS net_revenue,

          --count(DISTINCT transaction_id) Transactions,

          --count(transaction_id) count,
          --sum("user_rate") rate,
          sum(actual_postage_cost) postage_cost,
          COALESCE(SUM(license_fee_usd), 0) AS "Subscription_Revenue_USD",
          COALESCE(SUM(user_rev_share_usd), 0) AS "user_rev_share_usd",
          COALESCE(SUM(total_actual_label_cost_usd),0) as total_actual_label_cost_usd

      FROM
          (
              WITH query_variables AS
                       (SELECT
                            -- Date filter for whole query --> What dates range are we looking to pull?
                            '20230801' AS query_start_filter,
                            to_char(cast(current_date as date),'YYYYMMDD') AS query_end_filter,
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
                  breakage_indicator,
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
                  COALESCE((CASE WHEN ( CASE
                                            WHEN ttd.transaction_type = 'Dummy Label' THEN 'Subscription w/o Label'
                                            ELSE ttd.transaction_type
                      END )='Surcharge' AND  cd.carrier_name  ='USPS' and  ptd.parcel_type  ='return' THEN 0 ELSE lf.postage_cost_usd END), 0) AS "total_actual_label_cost_usd",
                  ----lf.total_actual_label_cost_usd,

      -- Transaction Type (need to determine inv chrg/ refund)
                  CASE
                      WHEN (rsd.refund_status IS NOT NULL OR ttd.transaction_type = 'Refund'OR ttd.transaction_type = 'Carrier Refund' OR ttd.transaction_type = 'Customer Refund') AND ptd.parcel_type = 'return' THEN 'return/refund'
                      WHEN (rsd.refund_status IS NOT NULL OR ttd.transaction_type = 'Refund'OR ttd.transaction_type = 'Carrier Refund' OR ttd.transaction_type = 'Customer Refund') THEN 'outbound/refund'
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
          )
      --carrier_own_account_indicator<>'Managed 3rd Party Master Account'
      where registration_source_mapped in ('snapfulfil')
        and user_id not in ('117603','186650')
        --and transaction_type in ('Purchase','Refund')
      GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13


      union
      --registration_source_mapped in ('skulabs')
      -- and entry_method_type='API'
      SELECT
          'Partner Commissions' as Category,
            case when registration_source_mapped in ('1440','boxstorm','fishbowl','freestyle','integrasoft','orangemarmalade','spoton','veeqo','zibbet') then 'RegSource'
         when registration_source_mapped in ('square') then 'WeeblySquare'
        when registration_source_mapped in ('loupe tech inc.') then 'loupe tech inc.'
         when registration_source_mapped in ('westerncomputer','commerce7') then 'Other'
         when registration_source_mapped in ('snapfulfil') then 'snapfulfil'
         when registration_source_mapped in ('woocommerce','godaddy','bigcommerce') then 'StorePlatforms'
         when registration_source_mapped in ('skulabs') then 'API'
         when store_platform_name in ('wix') and (entry_method_type) <> 'WIX-ELEMENTS' then 'Wix Platform'
         when entry_method_type in ('WIX-ELEMENTS') then 'Wix Elements' else 'unknown' end as Partner_Commission_Group,
          --transaction_id,
          --user_id,
          entry_method_type,
          company_name,
          store_platform_name  AS store_platform_name,
          registration_source_mapped,
          plan_name,
          purchase_date_mon,
          carrier_name,
          carrier_own_account_indicator,
breakage_indicator,
          user_payment_method,
          --platform_name,

          --carrier_service_level_name,
          transaction_type,
          sum(quantity) as Labels_count,
          COALESCE(SUM((CASE
                            WHEN  carrier_own_account_indicator   IN ('Managed 3rd Party Master Account') THEN 0 --CeC
                            WHEN  carrier_own_account_indicator   NOT IN ('Customer Own Account','PARTNER_BYOA','Managed 3rd Party Master Account','Marketplace Account')
                                AND  carrier_account_id   != 1524
                                THEN user_rate+COALESCE((CASE WHEN  provider_id   = 33 THEN 0.9 * DECODE(est_postage_cost, 0, actual_postage_cost, est_postage_cost) ELSE  DECODE(est_postage_cost, 0, actual_postage_cost, est_postage_cost) END), 0)
                            ELSE
                                0
              END) ), 0) AS label_markup,
          COALESCE(SUM(insurance_price_usd + COALESCE(insurance_cost_usd,0) ), 0) AS insurance_markup,
          COALESCE(SUM(label_fee_usd), 0) AS total_label_fee_usd,
          COALESCE(SUM(CASE WHEN NOT (carrier_account_id  = 1524) THEN carrier_partner_rev_share_usd  ELSE NULL END), 0) AS carrier_partner_rev_share_usd,
          COALESCE(SUM(CASE WHEN purchase_date_dim_id >=20180401 THEN
                                payment_provider_fee
                            WHEN payment_method='CREDIT_CARD' THEN
                                    -0.02 *(postage_revenue_usd + label_fee_usd + insurance_price_usd)
                            WHEN payment_method='P2P' THEN
                                    -0.015 *(postage_revenue_usd + label_fee_usd + insurance_price_usd)
                            ELSE 0 END
                       ), 0) AS total_payment_provider_fee_usd,
          COALESCE(SUM(carrier_referral_fee_usd ), 0) AS carrier_referral_fee_usd,
          COALESCE(SUM(user_rate),0) as total_postage_price_usd,
          COALESCE(SUM((CASE
                            WHEN  carrier_own_account_indicator   IN ('Managed 3rd Party Master Account') THEN NVL(user_rev_share_usd,0) --CeC
                            WHEN  carrier_own_account_indicator   NOT IN ('Customer Own Account','PARTNER_BYOA','Managed 3rd Party Master Account','Marketplace Account')
                                AND  carrier_account_id   != 1524
                                THEN user_rate
                                         + DECODE(est_postage_cost, 0, actual_postage_cost, est_postage_cost) + user_rev_share_usd
                                - (CASE WHEN  provider_id   = 33 THEN 0.1 * DECODE(est_postage_cost, 0, actual_postage_cost, est_postage_cost) ELSE 0 END)
                            ELSE 0
              END)
              + label_fee_usd
              +insurance_price_usd
              + address_validation_revenue_usd
              +license_fee_usd
              + insurance_cost_usd
              + carrier_partner_rev_share_usd
              + carrier_referral_fee_usd
                       --       + label_fact_fin.payment_provider_fee
                       ), 0) AS net_revenue,

          --count(DISTINCT transaction_id) Transactions,

          --count(transaction_id) count,
          --sum("user_rate") rate,
          sum(actual_postage_cost) postage_cost,
          COALESCE(SUM(license_fee_usd), 0) AS "Subscription_Revenue_USD",
          COALESCE(SUM(user_rev_share_usd), 0) AS "user_rev_share_usd",
          COALESCE(SUM(total_actual_label_cost_usd),0) as total_actual_label_cost_usd

      FROM
          (
              WITH query_variables AS
                       (SELECT
                            -- Date filter for whole query --> What dates range are we looking to pull?
                            '20230801' AS query_start_filter,
                            to_char(cast(current_date as date),'YYYYMMDD') AS query_end_filter,
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
                  lf.breakage_indicator,
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
                  COALESCE((CASE WHEN ( CASE
                                            WHEN ttd.transaction_type = 'Dummy Label' THEN 'Subscription w/o Label'
                                            ELSE ttd.transaction_type
                      END )='Surcharge' AND  cd.carrier_name  ='USPS' and  ptd.parcel_type  ='return' THEN 0 ELSE lf.postage_cost_usd END), 0) AS "total_actual_label_cost_usd",
                  ----lf.total_actual_label_cost_usd,

      -- Transaction Type (need to determine inv chrg/ refund)
                  CASE
                      WHEN (rsd.refund_status IS NOT NULL OR ttd.transaction_type = 'Refund'OR ttd.transaction_type = 'Carrier Refund' OR ttd.transaction_type = 'Customer Refund') AND ptd.parcel_type = 'return' THEN 'return/refund'
                      WHEN (rsd.refund_status IS NOT NULL OR ttd.transaction_type = 'Refund'OR ttd.transaction_type = 'Carrier Refund' OR ttd.transaction_type = 'Customer Refund') THEN 'outbound/refund'
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
          )
      --carrier_own_account_indicator<>'Managed 3rd Party Master Account'
      where registration_source_mapped in ('skulabs')
        and entry_method_type='API'
        --and transaction_type in ('Purchase','Refund')
      GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13

      union
      --store_platform_name in ('Bigcommerce','Godaddy','WooCommerce')
      SELECT
          'Partner Commissions' as Category,
            case when registration_source_mapped in ('1440','boxstorm','fishbowl','freestyle','integrasoft','orangemarmalade','spoton','veeqo','zibbet') then 'RegSource'
         when registration_source_mapped in ('square') then 'WeeblySquare'
        when registration_source_mapped in ('loupe tech inc.') then 'loupe tech inc.'
         when registration_source_mapped in ('westerncomputer','commerce7') then 'Other'
         when registration_source_mapped in ('snapfulfil') then 'snapfulfil'
         when registration_source_mapped in ('woocommerce','godaddy','bigcommerce') then 'StorePlatforms'
         when registration_source_mapped in ('skulabs') then 'API'
         when store_platform_name in ('wix') and (entry_method_type) <> 'WIX-ELEMENTS' then 'Wix Platform'
         when entry_method_type in ('WIX-ELEMENTS') then 'Wix Elements' else 'unknown' end as Partner_Commission_Group,
          --transaction_id,
          --user_id,
          entry_method_type,
          company_name,
          store_platform_name  AS store_platform_name,
          registration_source_mapped,
          plan_name,
          purchase_date_mon,
          carrier_name,
          carrier_own_account_indicator,
breakage_indicator,
          user_payment_method,
          --platform_name,

          --carrier_service_level_name,
          transaction_type,
          sum(quantity) as Labels_count,
          COALESCE(SUM((CASE
                            WHEN  carrier_own_account_indicator   IN ('Managed 3rd Party Master Account') THEN 0 --CeC
                            WHEN  carrier_own_account_indicator   NOT IN ('Customer Own Account','PARTNER_BYOA','Managed 3rd Party Master Account','Marketplace Account')
                                AND  carrier_account_id   != 1524
                                THEN user_rate+COALESCE((CASE WHEN  provider_id   = 33 THEN 0.9 * DECODE(est_postage_cost, 0, actual_postage_cost, est_postage_cost) ELSE  DECODE(est_postage_cost, 0, actual_postage_cost, est_postage_cost) END), 0)
                            ELSE
                                0
              END) ), 0) AS label_markup,
          COALESCE(SUM(insurance_price_usd + COALESCE(insurance_cost_usd,0) ), 0) AS insurance_markup,
          COALESCE(SUM(label_fee_usd), 0) AS total_label_fee_usd,
          COALESCE(SUM(CASE WHEN NOT (carrier_account_id  = 1524) THEN carrier_partner_rev_share_usd  ELSE NULL END), 0) AS carrier_partner_rev_share_usd,
          COALESCE(SUM(CASE WHEN purchase_date_dim_id >=20180401 THEN
                                payment_provider_fee
                            WHEN payment_method='CREDIT_CARD' THEN
                                    -0.02 *(postage_revenue_usd + label_fee_usd + insurance_price_usd)
                            WHEN payment_method='P2P' THEN
                                    -0.015 *(postage_revenue_usd + label_fee_usd + insurance_price_usd)
                            ELSE 0 END
                       ), 0) AS total_payment_provider_fee_usd,
          COALESCE(SUM(carrier_referral_fee_usd ), 0) AS carrier_referral_fee_usd,
          COALESCE(SUM(user_rate),0) as total_postage_price_usd,
          COALESCE(SUM((CASE
                            WHEN  carrier_own_account_indicator   IN ('Managed 3rd Party Master Account') THEN NVL(user_rev_share_usd,0) --CeC
                            WHEN  carrier_own_account_indicator   NOT IN ('Customer Own Account','PARTNER_BYOA','Managed 3rd Party Master Account','Marketplace Account')
                                AND  carrier_account_id   != 1524
                                THEN user_rate
                                         + DECODE(est_postage_cost, 0, actual_postage_cost, est_postage_cost) + user_rev_share_usd
                                - (CASE WHEN  provider_id   = 33 THEN 0.1 * DECODE(est_postage_cost, 0, actual_postage_cost, est_postage_cost) ELSE 0 END)
                            ELSE 0
              END)
              + label_fee_usd
              +insurance_price_usd
              + address_validation_revenue_usd
              +license_fee_usd
              + insurance_cost_usd
              + carrier_partner_rev_share_usd
              + carrier_referral_fee_usd
                       --       + label_fact_fin.payment_provider_fee
                       ), 0) AS net_revenue,

          --count(DISTINCT transaction_id) Transactions,

          --count(transaction_id) count,
          --sum("user_rate") rate,
          sum(actual_postage_cost) postage_cost,
          COALESCE(SUM(license_fee_usd), 0) AS "Subscription_Revenue_USD",
          COALESCE(SUM(user_rev_share_usd), 0) AS "user_rev_share_usd",
          COALESCE(SUM(total_actual_label_cost_usd),0) as total_actual_label_cost_usd

      FROM
          (
              WITH query_variables AS
                       (SELECT
                            -- Date filter for whole query --> What dates range are we looking to pull?
                            '20230801' AS query_start_filter,
                            to_char(cast(current_date as date),'YYYYMMDD') AS query_end_filter,
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
                  lf.breakage_indicator,
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
                  COALESCE((CASE WHEN ( CASE
                                            WHEN ttd.transaction_type = 'Dummy Label' THEN 'Subscription w/o Label'
                                            ELSE ttd.transaction_type
                      END )='Surcharge' AND  cd.carrier_name  ='USPS' and  ptd.parcel_type  ='return' THEN 0 ELSE lf.postage_cost_usd END), 0) AS "total_actual_label_cost_usd",
                  ----lf.total_actual_label_cost_usd,

      -- Transaction Type (need to determine inv chrg/ refund)
                  CASE
                      WHEN (rsd.refund_status IS NOT NULL OR ttd.transaction_type = 'Refund'OR ttd.transaction_type = 'Carrier Refund' OR ttd.transaction_type = 'Customer Refund') AND ptd.parcel_type = 'return' THEN 'return/refund'
                      WHEN (rsd.refund_status IS NOT NULL OR ttd.transaction_type = 'Refund'OR ttd.transaction_type = 'Carrier Refund' OR ttd.transaction_type = 'Customer Refund') THEN 'outbound/refund'
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
                       LEFT JOIN  user_dim ud ON lf.user_dim_id = ud.user_dim_id
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
          )
      --carrier_own_account_indicator<>'Managed 3rd Party Master Account'
      where store_platform_name in ('Bigcommerce','Godaddy','WooCommerce')
        --and transaction_type in ('Purchase','Refund')
      GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
      union
--store_platform_name in ('wix') and (entry_method_type) <> 'wix-elements'
SELECT
    'Partner Commissions' as Category,
      case when registration_source_mapped in ('1440','boxstorm','fishbowl','freestyle','integrasoft','orangemarmalade','spoton','veeqo','zibbet') then 'RegSource'
         when registration_source_mapped in ('square') then 'WeeblySquare'
        when registration_source_mapped in ('loupe tech inc.') then 'loupe tech inc.'
         when registration_source_mapped in ('westerncomputer','commerce7') then 'Other'
         when registration_source_mapped in ('snapfulfil') then 'snapfulfil'
         when registration_source_mapped in ('woocommerce','godaddy','bigcommerce') then 'StorePlatforms'
         when registration_source_mapped in ('skulabs') then 'API'
         when store_platform_name in ('wix') and (entry_method_type) <> 'WIX-ELEMENTS' then 'Wix Platform'
         when entry_method_type in ('WIX-ELEMENTS') then 'Wix Elements' else 'unknown' end as Partner_Commission_Group,
    --transaction_id,
    --user_id,
    entry_method_type,
    company_name,
    store_platform_name  AS store_platform_name,
    registration_source_mapped,
    plan_name,
    purchase_date_mon,
    carrier_name,
    carrier_own_account_indicator,
breakage_indicator,
    user_payment_method,
    --platform_name,

    --carrier_service_level_name,
    transaction_type,
    sum(quantity) as Labels_count,
    COALESCE(SUM((CASE
                      WHEN  carrier_own_account_indicator   IN ('Managed 3rd Party Master Account') THEN 0 --CeC
                      WHEN  carrier_own_account_indicator   NOT IN ('Customer Own Account','PARTNER_BYOA','Managed 3rd Party Master Account','Marketplace Account')
                          AND  carrier_account_id   != 1524
                          THEN user_rate+COALESCE((CASE WHEN  provider_id   = 33 THEN 0.9 * DECODE(est_postage_cost, 0, actual_postage_cost, est_postage_cost) ELSE  DECODE(est_postage_cost, 0, actual_postage_cost, est_postage_cost) END), 0)
                      ELSE
                          0
        END) ), 0) AS label_markup,
    COALESCE(SUM(insurance_price_usd + COALESCE(insurance_cost_usd,0) ), 0) AS insurance_markup,
    COALESCE(SUM(label_fee_usd), 0) AS total_label_fee_usd,
    COALESCE(SUM(CASE WHEN NOT (carrier_account_id  = 1524) THEN carrier_partner_rev_share_usd  ELSE NULL END), 0) AS carrier_partner_rev_share_usd,
    COALESCE(SUM(CASE WHEN purchase_date_dim_id >=20180401 THEN
                          payment_provider_fee
                      WHEN payment_method='CREDIT_CARD' THEN
                              -0.02 *(postage_revenue_usd + label_fee_usd + insurance_price_usd)
                      WHEN payment_method='P2P' THEN
                              -0.015 *(postage_revenue_usd + label_fee_usd + insurance_price_usd)
                      ELSE 0 END
                 ), 0) AS total_payment_provider_fee_usd,
    COALESCE(SUM(carrier_referral_fee_usd ), 0) AS carrier_referral_fee_usd,
    COALESCE(SUM(user_rate),0) as total_postage_price_usd,
    COALESCE(SUM((CASE
                      WHEN  carrier_own_account_indicator   IN ('Managed 3rd Party Master Account') THEN NVL(user_rev_share_usd,0) --CeC
                      WHEN  carrier_own_account_indicator   NOT IN ('Customer Own Account','PARTNER_BYOA','Managed 3rd Party Master Account','Marketplace Account')
                          AND  carrier_account_id   != 1524
                          THEN user_rate
                                   + DECODE(est_postage_cost, 0, actual_postage_cost, est_postage_cost) + user_rev_share_usd
                          - (CASE WHEN  provider_id   = 33 THEN 0.1 * DECODE(est_postage_cost, 0, actual_postage_cost, est_postage_cost) ELSE 0 END)
                      ELSE 0
        END)
        + label_fee_usd
        +insurance_price_usd
        + address_validation_revenue_usd
        +license_fee_usd
        + insurance_cost_usd
        + carrier_partner_rev_share_usd
        + carrier_referral_fee_usd
                 --       + label_fact_fin.payment_provider_fee
                 ), 0) AS net_revenue,

    --count(DISTINCT transaction_id) Transactions,

    --count(transaction_id) count,
    --sum("user_rate") rate,
    sum(actual_postage_cost) postage_cost,
    COALESCE(SUM(license_fee_usd), 0) AS "Subscription_Revenue_USD",
    COALESCE(SUM(user_rev_share_usd), 0) AS "user_rev_share_usd",
    COALESCE(SUM(total_actual_label_cost_usd),0) as total_actual_label_cost_usd

FROM
    (
        WITH query_variables AS
                 (SELECT
                      -- Date filter for whole query --> What dates range are we looking to pull?
                      '20230801' AS query_start_filter,
                      to_char(cast(current_date as date),'YYYYMMDD') AS query_end_filter,
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
            breakage_indicator,
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
            COALESCE((CASE WHEN ( CASE
                                      WHEN ttd.transaction_type = 'Dummy Label' THEN 'Subscription w/o Label'
                                      ELSE ttd.transaction_type
                END )='Surcharge' AND  cd.carrier_name  ='USPS' and  ptd.parcel_type  ='return' THEN 0 ELSE lf.postage_cost_usd END), 0) AS "total_actual_label_cost_usd",
            ----lf.total_actual_label_cost_usd,

-- Transaction Type (need to determine inv chrg/ refund)
            CASE
                WHEN (rsd.refund_status IS NOT NULL OR ttd.transaction_type = 'Refund'OR ttd.transaction_type = 'Carrier Refund' OR ttd.transaction_type = 'Customer Refund') AND ptd.parcel_type = 'return' THEN 'return/refund'
                WHEN (rsd.refund_status IS NOT NULL OR ttd.transaction_type = 'Refund'OR ttd.transaction_type = 'Carrier Refund' OR ttd.transaction_type = 'Customer Refund') THEN 'outbound/refund'
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
                 LEFT JOIN  user_dim ud ON lf.user_dim_id = ud.user_dim_id
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
    )
--carrier_own_account_indicator<>'Managed 3rd Party Master Account'
where ((entry_method_type) <> 'wix-elements' OR (entry_method_type ) IS NULL)
  AND (store_platform_name ) = 'Wix'
  --and transaction_type in ('Purchase','Refund')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
union
SELECT
    'Partner Commissions' as Category,
     case when registration_source_mapped in ('1440','boxstorm','fishbowl','freestyle','integrasoft','orangemarmalade','spoton','veeqo','zibbet') then 'RegSource'
         when registration_source_mapped in ('square') then 'WeeblySquare'
        when registration_source_mapped in ('loupe tech inc.') then 'loupe tech inc.'
         when registration_source_mapped in ('westerncomputer','commerce7') then 'Other'
         when registration_source_mapped in ('snapfulfil') then 'snapfulfil'
         when registration_source_mapped in ('woocommerce','godaddy','bigcommerce') then 'StorePlatforms'
         when registration_source_mapped in ('skulabs') then 'API'
         when store_platform_name in ('wix') and (entry_method_type) <> 'WIX-ELEMENTS' then 'Wix Platform'
         when entry_method_type in ('WIX-ELEMENTS') then 'Wix Elements' else 'unknown' end as Partner_Commission_Group,
    --transaction_id,
    --user_id,
    entry_method_type,
    company_name,
    store_platform_name  AS store_platform_name,
    registration_source_mapped,
    plan_name,
    purchase_date_mon,
    carrier_name,
    carrier_own_account_indicator,
breakage_indicator,
    user_payment_method,
    --platform_name,

    --carrier_service_level_name,
    transaction_type,
    sum(quantity) as Labels_count,
    COALESCE(SUM((CASE
                      WHEN  carrier_own_account_indicator   IN ('Managed 3rd Party Master Account') THEN 0 --CeC
                      WHEN  carrier_own_account_indicator   NOT IN ('Customer Own Account','PARTNER_BYOA','Managed 3rd Party Master Account','Marketplace Account')
                          AND  carrier_account_id   != 1524
                          THEN user_rate+COALESCE((CASE WHEN  provider_id   = 33 THEN 0.9 * DECODE(est_postage_cost, 0, actual_postage_cost, est_postage_cost) ELSE  DECODE(est_postage_cost, 0, actual_postage_cost, est_postage_cost) END), 0)
                      ELSE
                          0
        END) ), 0) AS label_markup,
    COALESCE(SUM(insurance_price_usd + COALESCE(insurance_cost_usd,0) ), 0) AS insurance_markup,
    COALESCE(SUM(label_fee_usd), 0) AS total_label_fee_usd,
    COALESCE(SUM(CASE WHEN NOT (carrier_account_id  = 1524) THEN carrier_partner_rev_share_usd  ELSE NULL END), 0) AS carrier_partner_rev_share_usd,
    COALESCE(SUM(CASE WHEN purchase_date_dim_id >=20180401 THEN
                          payment_provider_fee
                      WHEN payment_method='CREDIT_CARD' THEN
                              -0.02 *(postage_revenue_usd + label_fee_usd + insurance_price_usd)
                      WHEN payment_method='P2P' THEN
                              -0.015 *(postage_revenue_usd + label_fee_usd + insurance_price_usd)
                      ELSE 0 END
                 ), 0) AS total_payment_provider_fee_usd,
    COALESCE(SUM(carrier_referral_fee_usd ), 0) AS carrier_referral_fee_usd,
    COALESCE(SUM(user_rate),0) as total_postage_price_usd,
    COALESCE(SUM((CASE
                      WHEN  carrier_own_account_indicator   IN ('Managed 3rd Party Master Account') THEN NVL(user_rev_share_usd,0) --CeC
                      WHEN  carrier_own_account_indicator   NOT IN ('Customer Own Account','PARTNER_BYOA','Managed 3rd Party Master Account','Marketplace Account')
                          AND  carrier_account_id   != 1524
                          THEN user_rate
                                   + DECODE(est_postage_cost, 0, actual_postage_cost, est_postage_cost) + user_rev_share_usd
                          - (CASE WHEN  provider_id   = 33 THEN 0.1 * DECODE(est_postage_cost, 0, actual_postage_cost, est_postage_cost) ELSE 0 END)
                      ELSE 0
        END)
        + label_fee_usd
        +insurance_price_usd
        + address_validation_revenue_usd
        +license_fee_usd
        + insurance_cost_usd
        + carrier_partner_rev_share_usd
        + carrier_referral_fee_usd
                 --       + label_fact_fin.payment_provider_fee
                 ), 0) AS net_revenue,

    --count(DISTINCT transaction_id) Transactions,

    --count(transaction_id) count,
    --sum("user_rate") rate,
    sum(actual_postage_cost) postage_cost,
    COALESCE(SUM(license_fee_usd), 0) AS "Subscription_Revenue_USD",
    COALESCE(SUM(user_rev_share_usd), 0) AS "user_rev_share_usd",
    COALESCE(SUM(total_actual_label_cost_usd),0) as total_actual_label_cost_usd

FROM
    (
        WITH query_variables AS
                 (SELECT
                      -- Date filter for whole query --> What dates range are we looking to pull?
                            '20230801' AS query_start_filter,
                            to_char(cast(current_date as date),'YYYYMMDD') AS query_end_filter,
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
            breakage_indicator,
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
            COALESCE((CASE WHEN ( CASE
                                      WHEN ttd.transaction_type = 'Dummy Label' THEN 'Subscription w/o Label'
                                      ELSE ttd.transaction_type
                END )='Surcharge' AND  cd.carrier_name  ='USPS' and  ptd.parcel_type  ='return' THEN 0 ELSE lf.postage_cost_usd END), 0) AS "total_actual_label_cost_usd",
            ----lf.total_actual_label_cost_usd,

-- Transaction Type (need to determine inv chrg/ refund)
            CASE
                WHEN (rsd.refund_status IS NOT NULL OR ttd.transaction_type = 'Refund'OR ttd.transaction_type = 'Carrier Refund' OR ttd.transaction_type = 'Customer Refund') AND ptd.parcel_type = 'return' THEN 'return/refund'
                WHEN (rsd.refund_status IS NOT NULL OR ttd.transaction_type = 'Refund'OR ttd.transaction_type = 'Carrier Refund' OR ttd.transaction_type = 'Customer Refund') THEN 'outbound/refund'
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
                 LEFT JOIN  user_dim ud ON lf.user_dim_id = ud.user_dim_id
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
    )
--carrier_own_account_indicator<>'Managed 3rd Party Master Account'
where entry_method_type = 'WIX-ELEMENTS'

--and transaction_type in ('Purchase','Refund')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: category {
    type: string
    sql: ${TABLE}.category ;;
  }

  dimension: Partner_Commission_Group {
    type: string
    sql: ${TABLE}.Partner_Commission_Group ;;
  }

  dimension: entry_method_type {
    type: string
    sql: ${TABLE}.entry_method_type ;;
  }

  dimension: company_name {
    type: string
    sql: ${TABLE}.company_name ;;
  }

  dimension: store_platform_name {
    type: string
    sql: ${TABLE}.store_platform_name ;;
  }

  dimension: registration_source_mapped {
    type: string
    sql: ${TABLE}.registration_source_mapped ;;
  }

  dimension: plan_name {
    type: string
    sql: ${TABLE}.plan_name ;;
  }

  dimension: purchase_date_mon {
    type: string
    sql: ${TABLE}.purchase_date_mon ;;
  }

  dimension: transaction_type {
    type: string
    sql: ${TABLE}.transaction_type ;;
  }

  dimension: carrier_name {
    type: string
    sql: ${TABLE}.carrier_name ;;
  }

  dimension: carrier_own_account_indicator {
    type: string
    sql: ${TABLE}.carrier_own_account_indicator ;;
  }

  dimension: breakage_indicator {
    type: string
    sql: ${TABLE}.breakage_indicator ;;
  }

  dimension: user_payment_method {
    type: string
    sql: ${TABLE}.user_payment_method ;;
  }

  measure: labels_count {
    type: sum
    sql: ${TABLE}.labels_count ;;
  }

  measure: label_markup {
    type: sum
    sql: ${TABLE}.label_markup ;;
  }

  measure: insurance_markup {
    type: sum
    sql: ${TABLE}.insurance_markup ;;
  }

  measure: total_label_fee_usd {
    type: sum
    sql: ${TABLE}.total_label_fee_usd ;;
  }

  measure: carrier_partner_rev_share_usd {
    type: sum
    sql: ${TABLE}.carrier_partner_rev_share_usd ;;
  }

  measure: total_payment_provider_fee_usd {
    type: sum
    sql: ${TABLE}.total_payment_provider_fee_usd ;;
  }

  measure: carrier_referral_fee_usd {
    type: sum
    sql: ${TABLE}.carrier_referral_fee_usd ;;
  }

  measure: total_postage_price_usd {
    type: sum
    sql: ${TABLE}.total_postage_price_usd ;;
  }

  measure: net_revenue {
    type: sum
    sql: ${TABLE}.net_revenue ;;
  }

  measure: postage_cost {
    type: sum
    sql: ${TABLE}.postage_cost ;;
  }

  measure: subscription_revenue_usd {
    type: sum
    sql: ${TABLE}.subscription_revenue_usd ;;
  }

  measure: user_rev_share_usd {
    type: sum
    sql: ${TABLE}.user_rev_share_usd ;;
  }

  measure: total_actual_label_cost_usd {
    type: sum
    sql: ${TABLE}.total_actual_label_cost_usd ;;
  }

  set: detail {
    fields: [
      category,
      Partner_Commission_Group,
      entry_method_type,
      company_name,
      store_platform_name,
      registration_source_mapped,
      plan_name,
      purchase_date_mon,
      carrier_name,
      carrier_own_account_indicator,
      breakage_indicator,
      user_payment_method,
      transaction_type,
      labels_count,
      label_markup,
      insurance_markup,
      total_label_fee_usd,
      carrier_partner_rev_share_usd,
      total_payment_provider_fee_usd,
      carrier_referral_fee_usd,
      total_postage_price_usd,
      net_revenue,
      postage_cost,
      subscription_revenue_usd,
      user_rev_share_usd,
      total_actual_label_cost_usd
    ]
  }
}
