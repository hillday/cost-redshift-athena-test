select /* TPC-DS query69.tpl 0.28 */  
  cd_gender,
  cd_marital_status,
  cd_education_status,
  count(*) cnt1,
  cd_purchase_estimate,
  count(*) cnt2,
  cd_credit_rating,
  count(*) cnt3
 from
  "tpcds_orc_1000".customer c,"tpcds_orc_1000".customer_address ca,"tpcds_orc_1000".customer_demographics
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  ca_state in ('NV','OK','NY') and
  cd_demo_sk = c.c_current_cdemo_sk
 group by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
 order by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
 limit 100;
