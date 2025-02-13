create definer = root@localhost view vw_invoice_details as
select `i`.`project_number`   AS `project_number`,
       `i`.`po_number`        AS `po_number`,
       NULL                   AS `detail_number`,
       NULL                   AS `line_number`,
       `p`.`name`             AS `project_name`,
       `po`.`vendor_name`     AS `vendor_name`,
       `i`.`invoice_number`   AS `invoice_number`,
       `i`.`total`            AS `invoice_total`,
       `i`.`transaction_date` AS `transaction_date`,
       `i`.`file_link`        AS `file_link`,
       `c`.`name`             AS `contact_name`
from (((`virtual_pm`.`invoice` `i` join `virtual_pm`.`purchase_order` `po`
        on (((`i`.`project_number` = `po`.`project_number`) and
             (`i`.`po_number` = `po`.`po_number`)))) join `virtual_pm`.`project` `p`
       on ((`po`.`project_number` = `p`.`project_number`))) left join `virtual_pm`.`contact` `c`
      on ((`po`.`contact_id` = `c`.`id`)))
order by `i`.`project_number`, `i`.`po_number`, NULL, NULL;

