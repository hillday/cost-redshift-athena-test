select /* TPC-DS query15.tpl 0.57 */  ca_zip
       ,sum(c_birth_day)
 from "tpcds_orc_1000".customer
     ,"tpcds_orc_1000".customer_address
     ,"tpcds_orc_1000".date_dim
 where  c_current_addr_sk = ca_address_sk 
 	and ( substring(ca_zip,1,5) in ('85669', '86197','88274','83405','86475',
                                   '85392', '85460', '80348', '81792')
 	      or ca_state in ('CA','WA','GA'))
 	and d_qoy = 1 and d_year = 2002
 group by ca_zip
 order by ca_zip
 limit 100;

