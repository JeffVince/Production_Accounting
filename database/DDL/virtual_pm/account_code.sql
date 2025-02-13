create table account_code
(
    id                  bigint unsigned auto_increment
        primary key,
    code                varchar(45)                         not null,
    budget_map_id       bigint unsigned                     null,
    tax_id              bigint unsigned                     null,
    account_description varchar(45)                         null,
    created_at          timestamp default CURRENT_TIMESTAMP not null,
    updated_at          timestamp default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    constraint id_UNIQUE
        unique (id),
    constraint fk_account_code_budget_map
        foreign key (budget_map_id) references budget_map (id)
            on update cascade on delete set null,
    constraint fk_aicp_code_tax_account
        foreign key (tax_id) references tax_account (id)
            on update cascade on delete set null
);

create index fk_tax_account_idx
    on account_code (tax_id);

