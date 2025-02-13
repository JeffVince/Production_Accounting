create table tax_account
(
    id            bigint unsigned auto_increment
        primary key,
    tax_code      varchar(45)                         not null,
    description   varchar(255)                        null,
    tax_ledger_id bigint unsigned                     null,
    created_at    timestamp default CURRENT_TIMESTAMP not null,
    updated_at    timestamp default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    constraint id_UNIQUE
        unique (id),
    constraint fk_tax_account_tax_ledger
        foreign key (tax_ledger_id) references tax_ledger (id)
            on update cascade on delete set null
);

