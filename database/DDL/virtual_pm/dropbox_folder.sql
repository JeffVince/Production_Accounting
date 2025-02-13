create table dropbox_folder
(
    id             bigint auto_increment
        primary key,
    project_number int unsigned null,
    po_number      int unsigned null,
    vendor_name    varchar(100) null,
    dropbox_path   varchar(255) null,
    share_link     varchar(255) null,
    constraint dropbox_folder_id_uindex
        unique (id)
);

