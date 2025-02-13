create definer = root@localhost view vw_contact_transactions as
select `po`.`project_number` AS `project_number`,
       `po`.`po_number`      AS `po_number`,
       `di`.`detail_number`  AS `detail_number`,
       `di`.`line_number`    AS `line_number`,
       `c`.`id`              AS `contact_id`,
       `c`.`name`            AS `contact_name`,
       `p`.`name`            AS `project_name`,
       `po`.`vendor_name`    AS `vendor_name`,
       `di`.`description`    AS `detail_description`,
       `i`.`invoice_number`  AS `invoice_number`,
       `i`.`total`           AS `invoice_total`,
       `r`.`total`           AS `receipt_total`,
       `sm`.`amount`         AS `spend_amount`
from ((((((`virtual_pm`.`contact` `c` join `virtual_pm`.`purchase_order` `po`
           on ((`po`.`contact_id` = `c`.`id`))) join `virtual_pm`.`project` `p`
          on ((`po`.`project_number` = `p`.`project_number`))) left join `virtual_pm`.`detail_item` `di`
         on (((`di`.`project_number` = `po`.`project_number`) and
              (`di`.`po_number` = `po`.`po_number`)))) left join `virtual_pm`.`invoice` `i`
        on ((`di`.`invoice_id` = `i`.`id`))) left join `virtual_pm`.`receipt` `r`
       on ((`di`.`receipt_id` = `r`.`id`))) left join `virtual_pm`.`spend_money` `sm`
      on (((`sm`.`project_number` = `po`.`project_number`) and (`sm`.`po_number` = `po`.`po_number`) and
           (`sm`.`detail_number` = `di`.`detail_number`) and (`sm`.`line_number` = `di`.`line_number`))))
order by `po`.`project_number`, `po`.`po_number`, `di`.`detail_number`, `di`.`line_number`;

