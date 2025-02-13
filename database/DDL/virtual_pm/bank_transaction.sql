create table bank_transaction
(
    id                     bigint unsigned auto_increment
        primary key,
    mercury_transaction_id varchar(100)                          not null,
    state                  varchar(45) default 'Pending'         not null,
    xero_bill_id           bigint unsigned                       not null,
    created_at             timestamp   default CURRENT_TIMESTAMP not null,
    updated_at             timestamp   default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    xero_spend_money_id    bigint unsigned                       not null,
    constraint id_UNIQUE
        unique (id),
    constraint mercury_transaction_id
        unique (mercury_transaction_id),
    constraint bank_transaction_xero_spend_money_id_fk
        foreign key (xero_spend_money_id) references spend_money (id)
            on update cascade on delete cascade
);

create index fk_xero_bills_idx
    on bank_transaction (xero_bill_id);

create index fk_xero_spend_money_idx
    on bank_transaction (xero_spend_money_id);

