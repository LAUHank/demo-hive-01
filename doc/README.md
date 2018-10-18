# hive

进入hive

show databases;

create database lhl13;

use lhl13;

show tables;

## student

create table student (
id int, 
name string, 
score double, 
teacher_id int 
) ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH './student' OVERWRITE INTO TABLE student;

select teacher_id,score,name,id from student;

## teacher

create table teacher (
id int, 
name string 
) ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH './teacher' OVERWRITE INTO TABLE teacher;

select name, id from teacher;

## student_score

create table student_score (
id int, 
sname string, 
cname string, 
score double
) ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH './student_score' OVERWRITE INTO TABLE student_score;

select score, cname, sname, id from student_score;
-- ==================================
select sname,
max(
case cname
when 'Java' then score
else 0
end
) Java, 
max(
case cname
when 'MySQL' then score
else 0
end
) MySQL 
from student_score
group by sname;
-- ==================================
-- Hive中没有group_concat()函数，借助collect_list()、collect_set()
select sname,
concat_ws(' | ', collect_list(cname)), 
concat_ws(' | ', collect_list(cast(score as string))) 
from student_score
group by sname;

## demo

create table demo (
id int, 
name array<string>, 
score map<string, double>, 
addr struct<province:string, city:string>
) ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
COLLECTION ITEMS TERMINATED BY ',' 
MAP KEYS TERMINATED BY ':' ;

LOAD DATA LOCAL INPATH './demo' OVERWRITE INTO TABLE demo;

select addr, score, name, id from demo;

select id, name[0], score['all'] , score['语文'], addr.province from demo;

## classic table definition

以下是一个典型的hive表结构，下面的FIELDS、COLLECTION ITEMS、MAP KEYS、LINES的分隔符都是默认值，我们也可以根据实际场景修改（但是LINES的分隔符目前只支持\n）
create table my_demo_0 (
id int, 
name string, 
salary double, 
nick_name array<string>, 
score map<string, double>, 
addr struct<province:string, city:string> 
) ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\001' 
COLLECTION ITEMS TERMINATED BY '\002' 
MAP KEYS TERMINATED BY '\003' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

## use select to insert data

### basic data type

create table my_tbl (
id int, 
name string, 
salary double
);

INSERT INTO TABLE my_tbl
select * from (
SELECT 1, 'abc', 5.6 
union all 
SELECT 2, 'def', 7.8 
) t;

INSERT OVERWRITE TABLE my_tbl
select * from (
SELECT 3, '刘十三', 5.6 
union all 
SELECT 4, 'David', 7.8 
) t;

### array

create table my_data_type_2 (
id int, 
name array<string>
);

INSERT INTO my_data_type_2
SELECT 
2, 
array('小红', 'Lucy')
FROM my_demo LIMIT 1;

select id, name[0] from my_data_type_2;

### map
create table my_data_type_3 (
id int, 
score map<string, double>
);

INSERT INTO my_data_type_3
SELECT 
2, 
map('all', 28, '语文', 86.0, '数学', 98.0, '英语', 100.0)
FROM my_demo LIMIT 1;

select id, score['all'] ,score['语文']  from my_data_type_3;
