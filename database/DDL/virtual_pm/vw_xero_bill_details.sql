create definer = root@localhost view vw_xero_bill_details as
select `xb`.`project_number`        AS `project_number`,
       `xb`.`po_number`             AS `po_number`,
       `xb`.`detail_number`         AS `detail_number`,
       `xbl`.`line_number`          AS `line_number`,
       `p`.`name`                   AS `project_name`,
       `po`.`vendor_name`           AS `vendor_name`,
       `xb`.`transaction_date`      AS `transaction_date`,
       `xb`.`due_date`              AS `due_date`,
       `xb`.`xero_reference_number` AS `xero_reference_number`,
       `xbl`.`description`          AS `line_item_description`,
       `xbl`.`quantity`             AS `quantity`,
       `xbl`.`unit_amount`          AS `unit_amount`,
       `xbl`.`line_amount`          AS `line_amount`
from (((`virtual_pm`.`xero_bill` `xb` join `virtual_pm`.`purchase_order` `po`
        on (((`xb`.`project_number` = `po`.`project_number`) and
             (`xb`.`po_number` = `po`.`po_number`)))) join `virtual_pm`.`project` `p`
       on ((`xb`.`project_number` = `p`.`project_number`))) join `virtual_pm`.`xero_bill_line_item` `xbl`
      on (((`xb`.`id` = `xbl`.`parent_id`) and (`xb`.`project_number` = `xbl`.`project_number`) and
           (`xb`.`po_number` = `xbl`.`po_number`) and (`xb`.`detail_number` = `xbl`.`detail_number`))))
order by `xb`.`project_number`, `xb`.`po_number`, `xb`.`detail_number`, `xbl`.`line_number`;

