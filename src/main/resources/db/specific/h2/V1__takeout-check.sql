create table imported (
    quick_hash varchar(255) not null
    ,source_file varchar(255) not null primary key
    ,name varchar(100)
    ,date varchar(10)
    ,keep int
) AS SELECT * FROM CSVREAD('src/main/resources/static/all-files.csv');

create index idx_im_hash on imported(quick_hash);
create index idx_im_name on imported(name);
create index idx_im_date on imported(date);

