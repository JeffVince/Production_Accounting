create definer = root@localhost view vw_detail_item_breakdown as
select `di`.`project_number`   AS `project_number`,
       `di`.`po_number`        AS `po_number`,
       `di`.`detail_number`    AS `detail_number`,
       `di`.`line_number`      AS `line_number`,
       `p`.`name`              AS `project_name`,
       `po`.`vendor_name`      AS `vendor_name`,
       `di`.`account_code`     AS `account_code`,
       `di`.`payment_type`     AS `payment_type`,
       `di`.`description`      AS `detail_description`,
       `di`.`state`            AS `state`,
       `di`.`transaction_date` AS `transaction_date`,
       `di`.`due_date`         AS `due_date`,
       `di`.`rate`             AS `rate`,
       `di`.`quantity`         AS `quantity`,
       `di`.`ot`               AS `ot`,
       `di`.`fringes`          AS `fringes`,
       `di`.`sub_total`        AS `sub_total`,
       `c`.`name`              AS `contact_name`
from (((`virtual_pm`.`detail_item` `di` join `virtual_pm`.`purchase_order` `po`
        on (((`di`.`project_number` = `po`.`project_number`) and
             (`di`.`po_number` = `po`.`po_number`)))) join `virtual_pm`.`project` `p`
       on ((`di`.`project_number` = `p`.`project_number`))) left join `virtual_pm`.`contact` `c`
      on ((`po`.`contact_id` = `c`.`id`)))
order by `di`.`project_number`, `di`.`po_number`, `di`.`detail_number`, `di`.`line_number`;

