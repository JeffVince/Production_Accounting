create table contact
(
    id              bigint unsigned auto_increment
        primary key,
    name            varchar(255)                                                                 not null,
    vendor_status   enum ('PENDING', 'TO VERIFY', 'APPROVED', 'ISSUE') default 'PENDING'         not null,
    payment_details varchar(255)                                       default 'PENDING'         not null,
    vendor_type     enum ('VENDOR', 'CC', 'PC', 'INDIVIDUAL', 'S-CORP', 'C-CORP')                null,
    email           varchar(100)                                                                 null,
    phone           varchar(45)                                                                  null,
    address_line_1  varchar(255)                                                                 null,
    address_line_2  varchar(255)                                                                 null,
    city            varchar(100)                                                                 null,
    zip             varchar(20)                                                                  null,
    region          varchar(45)                                                                  null,
    country         varchar(100)                                                                 null,
    tax_type        varchar(45)                                        default 'SSN'             null,
    tax_number      varchar(45)                                                                  null,
    tax_form_id     bigint                                                                       null,
    pulse_id        bigint                                                                       null,
    xero_id         varchar(255)                                                                 null,
    created_at      timestamp                                          default CURRENT_TIMESTAMP not null,
    updated_at      timestamp                                          default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    constraint contact_tax_form_id_uindex
        unique (tax_form_id),
    constraint contact_xero_id_uindex
        unique (xero_id),
    constraint id_UNIQUE
        unique (id),
    constraint pulse_id
        unique (pulse_id)
);

