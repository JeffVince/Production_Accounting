create table purchase_order
(
    id             int unsigned auto_increment,
    project_number int unsigned                        not null,
    po_number      int unsigned                        not null,
    vendor_name    varchar(100)                        null,
    description    varchar(255)                        null,
    po_type        varchar(45)                         null,
    producer       varchar(100)                        null,
    pulse_id       bigint                              null,
    folder_link    varchar(255)                        null,
    contact_id     bigint unsigned                     null,
    project_id     bigint unsigned                     not null,
    created_at     timestamp default CURRENT_TIMESTAMP not null,
    updated_at     timestamp default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    primary key (project_number, po_number),
    constraint id_UNIQUE
        unique (id),
    constraint unique_project_po
        unique (project_number, po_number),
    constraint fk_purchase_order_contact
        foreign key (contact_id) references contact (id)
            on delete set null,
    constraint fk_purchase_order_project
        foreign key (project_number) references project (project_number)
            on update cascade on delete cascade
);

