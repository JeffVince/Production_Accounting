create definer = root@localhost view vw_transaction_summary as
select `di`.`project_number`        AS `project_number`,
       `po`.`po_number`             AS `po_number`,
       `di`.`detail_number`         AS `detail_number`,
       `di`.`line_number`           AS `line_number`,
       `p`.`name`                   AS `project_name`,
       `po`.`vendor_name`           AS `vendor_name`,
       `di`.`description`           AS `detail_description`,
       `di`.`state`                 AS `detail_state`,
       `i`.`invoice_number`         AS `invoice_number`,
       `i`.`total`                  AS `invoice_total`,
       `r`.`total`                  AS `receipt_total`,
       `sm`.`amount`                AS `spend_amount`,
       `xb`.`xero_reference_number` AS `xero_reference_number`,
       `xbl`.`description`          AS `xero_bill_line_description`,
       `c`.`name`                   AS `contact_name`
from ((((((((`virtual_pm`.`detail_item` `di` join `virtual_pm`.`purchase_order` `po`
             on (((`di`.`project_number` = `po`.`project_number`) and
                  (`di`.`po_number` = `po`.`po_number`)))) join `virtual_pm`.`project` `p`
            on ((`po`.`project_number` = `p`.`project_number`))) left join `virtual_pm`.`invoice` `i`
           on ((`di`.`invoice_id` = `i`.`id`))) left join `virtual_pm`.`receipt` `r`
          on ((`di`.`receipt_id` = `r`.`id`))) left join `virtual_pm`.`spend_money` `sm`
         on (((`sm`.`project_number` = `di`.`project_number`) and (`sm`.`po_number` = `di`.`po_number`) and
              (`sm`.`detail_number` = `di`.`detail_number`) and
              (`sm`.`line_number` = `di`.`line_number`)))) left join `virtual_pm`.`xero_bill` `xb`
        on (((`sm`.`project_number` = `xb`.`project_number`) and (`sm`.`po_number` = `xb`.`po_number`) and
             (`sm`.`detail_number` = `xb`.`detail_number`)))) left join `virtual_pm`.`xero_bill_line_item` `xbl`
       on (((`xb`.`id` = `xbl`.`parent_id`) and (`xb`.`project_number` = `xbl`.`project_number`) and
            (`xb`.`po_number` = `xbl`.`po_number`) and
            (`xb`.`detail_number` = `xbl`.`detail_number`)))) left join `virtual_pm`.`contact` `c`
      on ((`po`.`contact_id` = `c`.`id`)))
order by `di`.`project_number`, `po`.`po_number`, `di`.`detail_number`, `di`.`line_number`;

