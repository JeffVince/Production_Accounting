create table budget_map
(
    id         bigint unsigned auto_increment
        primary key,
    map_name   varchar(100)                        not null,
    user_id    bigint unsigned                     not null,
    created_at timestamp default CURRENT_TIMESTAMP not null,
    updated_at timestamp default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP,
    constraint fk_budget_map_users
        foreign key (user_id) references users (id)
            on update cascade on delete cascade
);

