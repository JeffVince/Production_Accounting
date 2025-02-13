create definer = root@localhost view vw_vendor_details as
select `po`.`project_number` AS `project_number`,
       `po`.`po_number`      AS `po_number`,
       NULL                  AS `detail_number`,
       NULL                  AS `line_number`,
       `c`.`id`              AS `contact_id`,
       `c`.`name`            AS `vendor_name`,
       count(`po`.`id`)      AS `total_purchase_orders`,
       sum(`sm`.`amount`)    AS `total_spend`
from ((`virtual_pm`.`purchase_order` `po` join `virtual_pm`.`contact` `c`
       on ((`po`.`contact_id` = `c`.`id`))) left join `virtual_pm`.`spend_money` `sm`
      on (((`po`.`project_number` = `sm`.`project_number`) and (`po`.`po_number` = `sm`.`po_number`))))
group by `po`.`project_number`, `po`.`po_number`, `c`.`id`, `c`.`name`
order by `po`.`project_number`, `po`.`po_number`;

