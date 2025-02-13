create table project
(
    id             bigint unsigned auto_increment,
    user_id        int                                                 null,
    project_number int unsigned                                        not null
        primary key,
    name           varchar(100)                                        not null,
    status         enum ('Active', 'Closed') default 'Active'          not null,
    tax_ledger     varchar(45)                                         null,
    budget_map_id  varchar(45)                                         null,
    created_at     timestamp                 default CURRENT_TIMESTAMP not null,
    updated_at     timestamp                 default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    constraint id
        unique (id),
    constraint id_UNIQUE
        unique (id)
);

