create table audit_log
(
    id         bigint unsigned auto_increment
        primary key,
    table_id   bigint unsigned                     null,
    operation  varchar(10)                         not null,
    record_id  bigint unsigned                     null,
    message    varchar(255)                        null,
    created_at timestamp default CURRENT_TIMESTAMP not null,
    constraint audit_log_sys_table_id_fk
        foreign key (table_id) references sys_table (id)
            on update cascade on delete cascade
)
    charset = utf8mb4;

