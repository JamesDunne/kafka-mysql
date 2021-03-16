
create database if not exists courses character set = utf8;

create user if not exists 'username'@'localhost' identified by 'password';

grant all on courses.* to 'username'@'localhost';

use courses;
drop table if exists advisedBy;
drop table if exists course;
drop table if exists person;
drop table if exists taughtBy;
