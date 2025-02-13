create definer = root@localhost view vw_project_budget_and_tax as
select `p`.`project_number`       AS `project_number`,
       NULL                       AS `po_number`,
       NULL                       AS `detail_number`,
       NULL                       AS `line_number`,
       `p`.`name`                 AS `project_name`,
       `p`.`status`               AS `project_status`,
       `bm`.`map_name`            AS `map_name`,
       `ta`.`tax_code`            AS `tax_code`,
       `ta`.`description`         AS `tax_description`,
       `ac`.`code`                AS `account_code`,
       `ac`.`account_description` AS `account_description`,
       `tl`.`name`                AS `tax_ledger_name`
from ((((`virtual_pm`.`project` `p` left join `virtual_pm`.`budget_map` `bm`
         on ((`bm`.`id` = cast(`p`.`budget_map_id` as unsigned)))) left join `virtual_pm`.`account_code` `ac`
        on ((`ac`.`budget_map_id` = `bm`.`id`))) left join `virtual_pm`.`tax_account` `ta`
       on ((`ac`.`tax_id` = `ta`.`id`))) left join `virtual_pm`.`tax_ledger` `tl`
      on ((`tl`.`id` = `ta`.`tax_ledger_id`)))
order by `p`.`project_number`, NULL, NULL, NULL;

