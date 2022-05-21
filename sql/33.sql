select iss.i_brand_id brand_id
     ,iss.i_class_id class_id
     ,iss.i_category_id category_id
 from "tpcds_orc_1000".store_sales
     ,"tpcds_orc_1000".item iss
     ,"tpcds_orc_1000".date_dim d1
 where ss_item_sk = iss.i_item_sk
   and ss_sold_date_sk = d1.d_date_sk
   and d1.d_year between 1999 AND 1999 + 2
 limit 100;
