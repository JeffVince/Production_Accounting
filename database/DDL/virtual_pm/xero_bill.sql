create table xero_bill
(
    id                    bigint unsigned auto_increment
        primary key,
    state                 varchar(45) default 'Draft'           not null,
    project_number        int unsigned                          null,
    po_number             int unsigned                          null,
    detail_number         int unsigned                          null,
    transaction_date      date                                  null,
    contact_xero_id       varchar(255)                          null,
    due_date              date                                  null,
    xero_reference_number varchar(50) as (concat(lpad(`project_number`, 4, _utf8mb4'0'), _utf8mb4'_',
                                                 lpad(`po_number`, 2, _utf8mb4'0'), _utf8mb4'_',
                                                 lpad(`detail_number`, 2, _utf8mb4'0'))) stored,
    xero_id               varchar(255)                          null,
    xero_link             varchar(255)                          null,
    updated_at            timestamp   default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    created_at            timestamp   default CURRENT_TIMESTAMP not null,
    constraint id_UNIQUE
        unique (id),
    constraint number
        unique (project_number desc, po_number desc, detail_number asc),
    constraint xero_id
        unique (xero_reference_number(45))
);

