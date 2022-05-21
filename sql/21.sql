 select i_item_id,sum(ss_ext_sales_price) total_sales
 from
 	"tpcds_orc_1000".store_sales,
 	"tpcds_orc_1000".date_dim,
        "tpcds_orc_1000".customer_address,
        "tpcds_orc_1000".item
 where i_item_id in (select
     i_item_id
from "tpcds_orc_1000".item
where i_color in ('antique','orchid','beige'))
 and     ss_item_sk              = i_item_sk
 and     ss_sold_date_sk         = d_date_sk
 and     d_year                  = 2001
 and     d_moy                   = 3
 and     ss_addr_sk              = ca_address_sk
 and     ca_gmt_offset           = -6 
 group by i_item_id 
 limit 100;
