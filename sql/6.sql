select /* TPC-DS query98.tpl 0.32 */ i_item_id
      ,i_item_desc 
      ,i_category 
      ,i_class 
      ,i_current_price
      ,sum(ss_ext_sales_price) as itemrevenue 
      ,sum(ss_ext_sales_price)*100/sum(sum(ss_ext_sales_price)) over
          (partition by i_class) as revenueratio
from	
	"tpcds_orc_1000".store_sales
    	,"tpcds_orc_1000".item 
    	,"tpcds_orc_1000".date_dim
where 
	ss_item_sk = i_item_sk 
  	and i_category in ('Men', 'Music', 'Shoes')
  	and ss_sold_date_sk = d_date_sk
group by 
	i_item_id
        ,i_item_desc 
        ,i_category
        ,i_class
        ,i_current_price
order by 
	i_category
        ,i_class
        ,i_item_id
        ,i_item_desc
        ,revenueratio;

