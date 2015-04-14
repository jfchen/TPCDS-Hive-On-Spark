select i_item_id
      ,i_item_desc
      ,i_current_price
from item i
     join inventory inv on (inv.inv_item_sk = i.i_item_sk)
     join store_sales ss on (ss.ss_item_sk = i.i_item_sk)
     join date_dim dt on (dt.d_date_sk = inv.inv_date_sk)
where i_current_price between 75.0 and 75.0+30.0
and d_date between '2002-02-14' and date_add('2002-02-14',60)
and i_manufact_id in (606,339,587,661)
and inv_quantity_on_hand between 100 and 500
group by i_item_id,i_item_desc,i_current_price
order by i_item_id
limit 100;
