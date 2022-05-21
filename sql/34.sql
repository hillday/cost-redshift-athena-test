select 'store' channel, i_brand_id,i_class_id
     ,i_category_id,sum(ss_quantity*ss_list_price) sales
     , count(*) number_sales
from "tpcds_orc_1000".store_sales
   ,"tpcds_orc_1000".item
   ,"tpcds_orc_1000".date_dim
where ss_item_sk = i_item_sk
 and ss_sold_date_sk = d_date_sk
 and d_year = 1999+2 
 and d_moy = 11
group by i_brand_id,i_class_id,i_category_id
having sum(ss_quantity*ss_list_price) > 100
limit 100;
