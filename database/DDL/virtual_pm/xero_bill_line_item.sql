create table xero_bill_line_item
(
    id                bigint unsigned auto_increment
        primary key,
    project_number    int unsigned                        null,
    po_number         int unsigned                        null,
    detail_number     int unsigned                        null,
    line_number       int unsigned                        null,
    description       varchar(255)                        null,
    quantity          decimal                             null,
    unit_amount       decimal                             null,
    line_amount       decimal                             null,
    account_code      int                                 null,
    parent_id         bigint unsigned                     not null,
    xero_bill_line_id varchar(255)                        null,
    parent_xero_id    varchar(255)                        null,
    updated_at        timestamp default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    created_at        timestamp default CURRENT_TIMESTAMP not null,
    transaction_date  date                                null,
    due_date          date                                null,
    constraint id_UNIQUE
        unique (id),
    constraint xero_bill_line_item_project_number
        unique (project_number desc, po_number asc, detail_number desc, line_number desc),
    constraint bill_line_item_xero_bill_id_fk
        foreign key (parent_id) references xero_bill (id)
            on update cascade on delete cascade
);

