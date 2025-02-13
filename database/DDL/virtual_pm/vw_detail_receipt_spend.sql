create definer = root@localhost view vw_detail_receipt_spend as
select `di`.`project_number`     AS `project_number`,
       `di`.`po_number`          AS `po_number`,
       `di`.`detail_number`      AS `detail_number`,
       `di`.`line_number`        AS `line_number`,
       `di`.`account_code`       AS `account_code`,
       `di`.`vendor`             AS `vendor`,
       `di`.`payment_type`       AS `payment_type`,
       `di`.`description`        AS `detail_description`,
       `sm`.`id`                 AS `spend_money_id`,
       `r`.`total`               AS `receipt_total`,
       `r`.`status`              AS `receipt_status`,
       `di`.`sub_total`          AS `sub_total`,
       `di`.`state`              AS `detail_state`,
       `sm`.`amount`             AS `spend_amount`,
       `sm`.`state`              AS `spend_money_state`,
       `sm`.`description`        AS `spend_money_description`,
       `r`.`id`                  AS `receipt_id`,
       `r`.`receipt_description` AS `receipt_description`,
       `di`.`transaction_date`   AS `transaction_date`,
       `di`.`due_date`           AS `due_date`,
       `di`.`rate`               AS `rate`,
       `di`.`quantity`           AS `quantity`,
       `di`.`ot`                 AS `ot`,
       `di`.`fringes`            AS `fringes`,
       `r`.`purchase_date`       AS `purchase_date`,
       `r`.`dropbox_path`        AS `receipt_dropbox_path`,
       `r`.`file_link`           AS `receipt_file_link`,
       `sm`.`date`               AS `spend_money_date`,
       `sm`.`xero_link`          AS `spend_money_xero_link`
from ((`virtual_pm`.`detail_item` `di` left join `virtual_pm`.`receipt` `r`
       on (((`di`.`project_number` = `r`.`project_number`) and (`di`.`po_number` = `r`.`po_number`) and
            (`di`.`detail_number` = `r`.`detail_number`) and
            (`di`.`line_number` = `r`.`line_number`)))) left join `virtual_pm`.`spend_money` `sm`
      on (((`di`.`project_number` = `sm`.`project_number`) and (`di`.`po_number` = `sm`.`po_number`) and
           (`di`.`detail_number` = `sm`.`detail_number`) and (`di`.`line_number` = `sm`.`line_number`))))
order by `di`.`project_number`, `di`.`po_number`, `di`.`detail_number`, `di`.`line_number`;

