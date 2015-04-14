select c_last_name
       ,c_first_name
       ,c_salutation
       ,c_preferred_cust_flag
       ,ss_ticket_number
       ,cnt from
   (select ss_ticket_number
          ,ss_customer_sk
          ,count(*) cnt
    from store_sales
     JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
     JOIN store ON store_sales.ss_store_sk = store.s_store_sk
     JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    where
         date_dim.d_dom between 1 and 2
    and (household_demographics.hd_buy_potential = '1001-5000' or
         household_demographics.hd_buy_potential = 'unknown')
    and household_demographics.hd_vehicle_count > 0
    and case when household_demographics.hd_vehicle_count > 0 then
             household_demographics.hd_dep_count/ household_demographics.hd_vehicle_count else null end > 1
    and date_dim.d_year in (1998,1998+1,1998+2)
    and store.s_county in ('Williamson County','Jefferson Davis Parish','San Miguel County','Gage County')
    group by ss_ticket_number,ss_customer_sk) dj
  JOIN customer ON dj.ss_customer_sk = customer.c_customer_sk
  where
      cnt between 1 and 5
    order by cnt desc;
