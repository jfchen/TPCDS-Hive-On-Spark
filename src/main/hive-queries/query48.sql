select sum (ss_quantity)
 from store_sales
 JOIN store ON store.s_store_sk = store_sales.ss_store_sk
 JOIN customer_demographics ON customer_demographics.cd_demo_sk = store_sales.ss_cdemo_sk
 JOIN customer_address ON store_sales.ss_addr_sk = customer_address.ca_address_sk
 JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
 where
 d_year = 2000
 and
 (
  (
   cd_marital_status = 'M'
   and
   cd_education_status = '4 yr Degree'
   and
   ss_sales_price between 100.00 and 150.00
   )
 or
  (
   cd_marital_status = 'M'
   and
   cd_education_status = '4 yr Degree'
   and
   ss_sales_price between 50.00 and 100.00
  )
 or
 (
   cd_marital_status = 'M'
   and
   cd_education_status = '4 yr Degree'
   and
   ss_sales_price between 150.00 and 200.00
 )
 )
 and
 (
  (
  ca_country = 'United States'
  and
  ca_state in ('KY', 'GA', 'FL')
  and ss_net_profit between 0 and 2000
  )
 or
  (
  ca_country = 'United States'
  and
  ca_state in ('GA', 'FL', 'MT')
  and ss_net_profit between 150 and 3000
  )
 or
  (
  ca_country = 'United States'
  and
  ca_state in ('AR', 'SC', 'VA')
  and ss_net_profit between 50 and 25000
  )
 )
;

