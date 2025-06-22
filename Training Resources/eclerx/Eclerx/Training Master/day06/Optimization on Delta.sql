-- Databricks notebook source
create table emp (id int, name string, age int)

-- COMMAND ----------

insert into emp values (1,'a',20),(2,'b',30),(3,'c',30),(4,'d',30),(5,'e',30)

-- COMMAND ----------

insert into emp values (6,'a',20),(7,'b',30);
insert into emp values (6,'a',20),(7,'b',30);
insert into emp values (6,'a',20),(7,'b',30);
insert into emp values (6,'a',20),(7,'b',30);
insert into emp values (6,'a',20),(7,'b',30);
insert into emp values (6,'a',20),(7,'b',30);
insert into emp values (6,'a',20),(7,'b',30);
insert into emp values (6,'a',20),(7,'b',30)

-- COMMAND ----------

describe history emp

-- COMMAND ----------

describe detail emp

-- COMMAND ----------

describe extended emp

-- COMMAND ----------

select * from emp where id= 1

-- COMMAND ----------

select * from emp where id= 1

-- COMMAND ----------

compacting small files in large== optimze 

-- COMMAND ----------

optimize emp 
zorder by (id)

-- COMMAND ----------

describe history emp

-- COMMAND ----------

vacuum: to remove unused/ stale part files 
note: NO time travel 

-- COMMAND ----------

vacuum emp 

-- COMMAND ----------

select * from emp version as of 2

-- COMMAND ----------

Retention period for vacuum is 7 days= 168 hours 

-- COMMAND ----------

vacuum emp retain 0 hours

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

vacuum emp retain 0 hours dry run

-- COMMAND ----------

vacuum emp retain 0 hours

-- COMMAND ----------

select * from emp version as of 2

-- COMMAND ----------

A data engineer has realized that they made a mistake when making a daily update to a table. They need to use Delta time travel to restore the table to a version that is 3 days old. However, when the data engineer attempts to time travel to the older version, they are unable to restore the data because the data files have been deleted.
Which of the following explains why the data files are no longer present?
A. The VACUUM command was run on the table
B. The TIME TRAVEL command was run on the table
C. The DELETE HISTORY command was run on the table
D. The OPTIMIZE command was nun on the table
E. The HISTORY command was run on the table

