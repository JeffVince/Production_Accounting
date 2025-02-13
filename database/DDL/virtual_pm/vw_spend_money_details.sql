create definer = root@localhost view vw_spend_money_details as
select `sm`.`project_number` AS `project_number`,
       `sm`.`po_number`      AS `po_number`,
       `sm`.`detail_number`  AS `detail_number`,
       `sm`.`line_number`    AS `line_number`,
       `p`.`name`            AS `project_name`,
       `po`.`vendor_name`    AS `vendor_name`,
       `sm`.`amount`         AS `spend_amount`,
       `sm`.`description`    AS `description`,
       `sm`.`state`          AS `state`,
       `sm`.`date`           AS `date`,
       `sm`.`xero_link`      AS `xero_link`,
       `c`.`name`            AS `contact_name`
from (((`virtual_pm`.`spend_money` `sm` join `virtual_pm`.`purchase_order` `po`
        on (((`sm`.`project_number` = `po`.`project_number`) and
             (`sm`.`po_number` = `po`.`po_number`)))) join `virtual_pm`.`project` `p`
       on ((`sm`.`project_number` = `p`.`project_number`))) left join `virtual_pm`.`contact` `c`
      on ((`po`.`contact_id` = `c`.`id`)))
order by `sm`.`project_number`, `sm`.`po_number`, `sm`.`detail_number`, `sm`.`line_number`;

