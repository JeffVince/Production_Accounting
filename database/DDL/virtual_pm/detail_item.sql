create table detail_item
(
    id               bigint unsigned auto_increment,
    project_number   int unsigned                                                                                                                                    not null,
    po_number        int unsigned                                                                                                                                    not null,
    detail_number    int unsigned                                                                                                                                    not null,
    line_number      int unsigned                                                                                                                                    not null,
    account_code     varchar(45)                                                                                                                                     not null,
    vendor           varchar(255)                                                                                                                                    null,
    payment_type     varchar(45)                                                                                                                                     null,
    state            enum ('PENDING', 'OVERDUE', 'REVIEWED', 'ISSUE', 'RTP', 'RECONCILED', 'PAID', 'APPROVED', 'SUBMITTED', 'PO MISMATCH') default 'PENDING'         not null,
    description      varchar(255)                                                                                                                                    null,
    transaction_date datetime                                                                                                                                        null,
    due_date         datetime                                                                                                                                        null,
    rate             decimal(15, 2)                                                                                                                                  not null,
    quantity         decimal(15, 2)                                                                                                        default 1.00              not null,
    ot               decimal(15, 2)                                                                                                        default 0.00              null,
    fringes          decimal(15, 2)                                                                                                        default 0.00              null,
    sub_total        decimal(15, 2) as (round((((`rate` * `quantity`) + ifnull(`ot`, 0)) + ifnull(`fringes`, 0)),
                                              2)) stored,
    pulse_id         bigint                                                                                                                                          null,
    xero_id          varchar(100)                                                                                                                                    null,
    parent_pulse_id  bigint                                                                                                                                          null,
    created_at       timestamp                                                                                                             default CURRENT_TIMESTAMP not null,
    updated_at       timestamp                                                                                                             default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    primary key (project_number desc, po_number asc, detail_number asc, line_number asc),
    constraint detail_item_pulse_id_uindex
        unique (pulse_id),
    constraint id_UNIQUE
        unique (id),
    constraint detail_po_fk
        foreign key (project_number, po_number) references purchase_order (project_number, po_number)
            on update cascade on delete cascade
);

create index detail_item_project_number_po_number_index
    on detail_item (project_number asc, po_number desc);

