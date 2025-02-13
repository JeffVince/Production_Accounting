create table sys_table
(
    id                     bigint unsigned auto_increment
        primary key,
    name                   varchar(100)                                                                           not null,
    type                   enum ('SYSTEM', 'PARENT/CHILD', 'PARENT', 'CHILD', 'SINGLE') default 'SINGLE'          not null,
    integration_name       varchar(45)                                                                            null,
    integration_type       enum ('PARENT', 'CHILD', 'SINGLE')                           default 'SINGLE'          not null,
    integration_connection enum ('NONE', '1to1', '1toMany', 'Manyto1', 'ManytoMany')    default 'NONE'            not null,
    created_at             timestamp                                                    default CURRENT_TIMESTAMP not null,
    updated_at             timestamp                                                    default CURRENT_TIMESTAMP not null,
    constraint table_id_uindex
        unique (id)
);

