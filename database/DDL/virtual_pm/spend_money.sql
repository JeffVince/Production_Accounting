create table spend_money
(
    id                                bigint unsigned auto_increment
        primary key,
    project_number                    int unsigned                          null,
    po_number                         int unsigned                          null,
    detail_number                     int unsigned                          null,
    line_number                       int unsigned                          null,
    amount                            decimal(10, 2)                        null,
    description                       varchar(255)                          null,
    state                             varchar(45) default 'Draft'           not null,
    date                              date                                  null,
    contact_id                        bigint                                null,
    tax_code                          int                                   null,
    xero_link                         varchar(255)                          null,
    created_at                        timestamp   default CURRENT_TIMESTAMP not null,
    updated_at                        timestamp   default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    xero_spend_money_id               varchar(100)                          null,
    xero_spend_money_reference_number varchar(50) as (concat(lpad(`project_number`, 4, _utf8mb4'0'), _utf8mb4'_',
                                                             lpad(`po_number`, 2, _utf8mb4'0'), _utf8mb4'_',
                                                             lpad(`detail_number`, 2, _utf8mb4'0'), _utf8mb4'_',
                                                             lpad(`line_number`, 2, _utf8mb4'0'))) stored,
    constraint id_UNIQUE
        unique (id)
);

