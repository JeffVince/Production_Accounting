create definer = root@localhost view vw_po_log_details as
select `pl`.`project_number` AS `project_number`,
       NULL                  AS `po_number`,
       NULL                  AS `detail_number`,
       NULL                  AS `line_number`,
       `pl`.`filename`       AS `filename`,
       `pl`.`db_path`        AS `db_path`,
       `pl`.`status`         AS `status`,
       `pl`.`created_at`     AS `created_at`
from `virtual_pm`.`po_log` `pl`
order by `pl`.`project_number`, NULL, NULL, NULL;

