select /* TPC-DS query10.tpl 0.26 */  
  cd_gender,
  cd_marital_status,
  cd_education_status,
  count(*) cnt1,
  cd_purchase_estimate,
  count(*) cnt2,
  cd_credit_rating,
  count(*) cnt3,
  cd_dep_count,
  count(*) cnt4,
  cd_dep_employed_count,
  count(*) cnt5,
  cd_dep_college_count,
  count(*) cnt6
 from
  "tpcds_orc_1000".customer c,"tpcds_orc_1000".customer_address ca,"tpcds_orc_1000".customer_demographics
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  ca_county in ('Fayette County','Niagara County','Pike County','Morgan County','Union County') and
  cd_demo_sk = c.c_current_cdemo_sk and 
  exists (select *
          from "tpcds_orc_1000".store_sales,"tpcds_orc_1000".date_dim
          where c.c_customer_sk = ss_customer_sk and
                ss_sold_date_sk = d_date_sk and
                d_year = 2000 and
                d_moy between 4 and 4+3) 
 group by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
 order by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
limit 100;
