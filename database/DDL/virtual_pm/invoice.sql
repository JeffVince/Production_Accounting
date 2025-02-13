create table invoice
(
    id               bigint unsigned auto_increment
        primary key,
    project_number   int unsigned                             not null,
    po_number        int unsigned                             not null,
    invoice_number   int unsigned                             not null,
    term             int                                      null,
    total            decimal(15, 2) default 0.00              null,
    status           enum ('PENDING', 'VERIFIED', 'REJECTED') null,
    transaction_date datetime                                 null,
    file_link        varchar(255)                             null,
    created_at       timestamp      default CURRENT_TIMESTAMP not null,
    updated_at       timestamp      default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    constraint id_UNIQUE
        unique (id)
);

create index po_id
    on invoice (po_number);

