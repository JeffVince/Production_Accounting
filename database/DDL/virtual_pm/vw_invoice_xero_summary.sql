create definer = root@localhost view vw_invoice_xero_summary as
select `i`.`project_number`                                                                                 AS `project_number`,
       `i`.`po_number`                                                                                      AS `po_number`,
       NULL                                                                                                 AS `detail_number`,
       NULL                                                                                                 AS `line_number`,
       `i`.`id`                                                                                             AS `invoice_id`,
       `i`.`invoice_number`                                                                                 AS `invoice_number`,
       `i`.`total`                                                                                          AS `invoice_total`,
       `i`.`transaction_date`                                                                               AS `invoice_date`,
       count(`di`.`id`)                                                                                     AS `detail_count`,
       sum(`di`.`sub_total`)                                                                                AS `detail_total`,
       group_concat(distinct `xb`.`xero_reference_number` order by `xb`.`detail_number` ASC separator
                    ', ')                                                                                   AS `xero_references`
from ((`virtual_pm`.`invoice` `i` join `virtual_pm`.`detail_item` `di`
       on ((`di`.`invoice_id` = `i`.`id`))) left join `virtual_pm`.`xero_bill` `xb`
      on (((`di`.`project_number` = `xb`.`project_number`) and (`di`.`po_number` = `xb`.`po_number`) and
           (`di`.`detail_number` = `xb`.`detail_number`))))
group by `i`.`id`, `i`.`project_number`, `i`.`po_number`, `i`.`invoice_number`, `i`.`total`, `i`.`transaction_date`
order by `i`.`project_number`, `i`.`po_number`, `detail_number`, `line_number`;

