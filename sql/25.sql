select /* TPC-DS query6.tpl 0.58 */  a.ca_state state, count(*) cnt
 from "tpcds_orc_1000".customer_address a
     ,"tpcds_orc_1000".customer c
     ,"tpcds_orc_1000".store_sales s
     ,"tpcds_orc_1000".date_dim d
     ,"tpcds_orc_1000".item i
 where       a.ca_address_sk = c.c_current_addr_sk
 	and c.c_customer_sk = s.ss_customer_sk
 	and s.ss_sold_date_sk = d.d_date_sk
 	and s.ss_item_sk = i.i_item_sk
 	and d.d_month_seq = 
 	     (select distinct (d_month_seq)
 	      from "tpcds_orc_1000".date_dim
               where d_year = 2002
 	        and d_moy = 7 )
 	and i.i_current_price > 1.2 * 
             (select avg(j.i_current_price) 
 	     from "tpcds_orc_1000".item j 
 	     where j.i_category = i.i_category)
 group by a.ca_state
 having count(*) >= 10
 order by cnt, a.ca_state 
 limit 100;
