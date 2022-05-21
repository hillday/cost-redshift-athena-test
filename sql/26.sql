select /* TPC-DS query28.tpl 0.36 */  *
from (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from "tpcds_orc_1000".store_sales
      where ss_quantity between 0 and 5
        and (ss_list_price between 100 and 100+10 
             or ss_coupon_amt between 7313 and 7313+1000
             or ss_wholesale_cost between 71 and 71+20)) B1,
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from "tpcds_orc_1000".store_sales
      where ss_quantity between 6 and 10
        and (ss_list_price between 187 and 187+10
          or ss_coupon_amt between 2738 and 2738+1000
          or ss_wholesale_cost between 49 and 49+20)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from "tpcds_orc_1000".store_sales
      where ss_quantity between 11 and 15
        and (ss_list_price between 133 and 133+10
          or ss_coupon_amt between 4856 and 4856+1000
          or ss_wholesale_cost between 51 and 51+20)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from "tpcds_orc_1000".store_sales
      where ss_quantity between 16 and 20
        and (ss_list_price between 28 and 28+10
          or ss_coupon_amt between 14150 and 14150+1000
          or ss_wholesale_cost between 22 and 22+20)) B4,
     (select avg(ss_list_price) B5_LP
            ,count(ss_list_price) B5_CNT
            ,count(distinct ss_list_price) B5_CNTD
      from "tpcds_orc_1000".store_sales
      where ss_quantity between 21 and 25
        and (ss_list_price between 45 and 45+10
          or ss_coupon_amt between 15100 and 15100+1000
          or ss_wholesale_cost between 60 and 60+20)) B5,
     (select avg(ss_list_price) B6_LP
            ,count(ss_list_price) B6_CNT
            ,count(distinct ss_list_price) B6_CNTD
      from "tpcds_orc_1000".store_sales
      where ss_quantity between 26 and 30
        and (ss_list_price between 57 and 57+10
          or ss_coupon_amt between 10201 and 10201+1000
          or ss_wholesale_cost between 40 and 40+20)) B6
limit 100;
