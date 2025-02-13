create table po_log
(
    id             bigint unsigned auto_increment
        primary key,
    project_number int unsigned                                                                 null,
    filename       varchar(255)                                                                 null,
    db_path        varchar(255)                                                                 not null,
    status         enum ('PENDING', 'STARTED', 'COMPLETED', 'FAILED') default 'PENDING'         not null,
    created_at     timestamp                                          default CURRENT_TIMESTAMP not null,
    updated_at     timestamp                                          default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    constraint id_UNIQUE
        unique (id)
);

