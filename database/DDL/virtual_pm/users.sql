create table users
(
    id         bigint unsigned auto_increment
        primary key,
    username   varchar(100)                        not null,
    contact_id bigint unsigned                     null,
    created_at timestamp default CURRENT_TIMESTAMP not null,
    updated_at timestamp default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    constraint fk_user_contact
        foreign key (contact_id) references contact (id)
            on update cascade on delete set null
);

