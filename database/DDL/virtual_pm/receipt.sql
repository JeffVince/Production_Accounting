create table receipt
(
    id                  bigint unsigned auto_increment
        primary key,
    project_number      int unsigned                             not null,
    po_number           int unsigned                             null,
    detail_number       int unsigned                             null,
    line_number         int unsigned                             null,
    receipt_description varchar(255)                             null,
    total               decimal(15, 2) default 0.00              null,
    purchase_date       datetime                                 null,
    dropbox_path        varchar(255)                             null,
    status              enum ('PENDING', 'VERIFIED', 'REJECTED') null,
    file_link           varchar(255)                             not null,
    created_at          timestamp      default CURRENT_TIMESTAMP not null,
    updated_at          timestamp      default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    spend_money_id      bigint unsigned                          null,
    constraint id_UNIQUE
        unique (id),
    constraint receipt_spend_money_id_fk
        foreign key (spend_money_id) references spend_money (id)
            on update cascade on delete set null
);

create index fk_spend_money_idx
    on receipt (spend_money_id);

