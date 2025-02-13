create definer = root@localhost view vw_receipt_details as
select `r`.`project_number`      AS `project_number`,
       `r`.`po_number`           AS `po_number`,
       `r`.`detail_number`       AS `detail_number`,
       `r`.`line_number`         AS `line_number`,
       `p`.`name`                AS `project_name`,
       `po`.`vendor_name`        AS `vendor_name`,
       `r`.`receipt_description` AS `receipt_description`,
       `r`.`total`               AS `receipt_total`,
       `r`.`purchase_date`       AS `purchase_date`,
       `r`.`file_link`           AS `file_link`,
       `c`.`name`                AS `contact_name`
from (((`virtual_pm`.`receipt` `r` join `virtual_pm`.`purchase_order` `po`
        on (((`r`.`project_number` = `po`.`project_number`) and
             (`r`.`po_number` = `po`.`po_number`)))) join `virtual_pm`.`project` `p`
       on ((`r`.`project_number` = `p`.`project_number`))) left join `virtual_pm`.`contact` `c`
      on ((`po`.`contact_id` = `c`.`id`)))
order by `r`.`project_number`, `r`.`po_number`, `r`.`detail_number`, `r`.`line_number`;

