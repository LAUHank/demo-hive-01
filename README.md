# demo-hive-01

hive version 1.2.1
hadoop version 2.7.3

ADD JAR /home/hdfs/lhl13/hive/hive-jar-with-dependencies.jar;

create temporary function my_mask as 'cn.lhl.hive.udf.ValueMaskUDF';
select my_mask('James', 2, '...');

create temporary function my_count_2 as 'cn.lhl.hive.udaf.MyCountUDAF';
select my_count_2(1) from 
(
select 1,2 
union all 
select 2,3 
)t;

create temporary function my_count as 'cn.lhl.hive.udaf.DIYCountUDAF';
select my_count(1) from 
(
select 1,2 
union all 
select 2,3 
)t;

create temporary function my_agg as 'cn.lhl.hive.udaf.StudentScoreAggUDAF';
select sname, 
my_agg(cname, score) 
from student_score 
group by sname;

create temporary function my_min as 'cn.lhl.hive.udaf.MyMinUDAF';
select my_min(score) from student;
select teacher_id, my_min(score) 
from student 
group by teacher_id;
