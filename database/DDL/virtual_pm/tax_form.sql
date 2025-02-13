create table tax_form
(
    id            bigint unsigned auto_increment
        primary key,
    type          enum ('W9', 'W8-BEN', 'W8-BEN-E')                                 not null,
    status        enum ('VERIFIED', 'INVALID', 'PENDING') default 'PENDING'         not null,
    entity_name   varchar(100)                                                      not null,
    filename      varchar(100)                                                      null,
    db_path       varchar(255)                                                      null,
    tax_form_link varchar(255)                                                      null,
    created_at    timestamp                               default CURRENT_TIMESTAMP not null,
    updated_at    timestamp                               default CURRENT_TIMESTAMP not null,
    constraint tax_form_entity_name_uindex
        unique (entity_name),
    constraint tax_form_id_uindex
        unique (id),
    constraint tax_form_tax_form_link_uindex
        unique (tax_form_link)
);

