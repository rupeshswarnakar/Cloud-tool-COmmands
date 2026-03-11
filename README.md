# Cloud-tool-Commands

## HADOOP
```
Create a dir with a name in your $HOME dir and change dir with cd command
mkdir ~/data
cd ~/data

touch zipcodes.csv 
nano zipcodes.csv 
cltr +s
cltr +x

hadoop fs -mkdir -p /user/test/data
hadoop fs -put zipcodes.csv /user/test/data
hadoop fs -get /user/test/data/zipcodes.csv    
hadoop fs -cat /user/test/data/zipcodes.csv
hadoop fs -ls /user/test/data
hadoop fs -rm  /user/test/data/zipcodes.csv
hadoop fs -rm -r  /user/test/data
hadoop fs -cat  /user/test/data/zipcodes.csv | head -1
hadoop fs -du /user/test/data/zipcodes.csv
hadoop fs -mv /user/test/data/zipcodes.csv /data1
hadoop fs -moveFromLocal zipcodes1.csv /data2
```



## HIVE
```
create database xyz;
use xyz;
HADOOP
CREATE EXTERNAL TABLE
customer.txt
nano customer.txt
ctrl + s
ctrl + x
1,rahul,2010-01-01,2010-01-01 01:01:01
2,rakesh,2010-01-02,2010-01-02 01:01:01
,rahul,,2010-01-01 01:01:01
4,rakesh,2010-01-02,2010-01-02 01:01:01

hadoop fs -mkdir -p /data/test/text
hadoop fs -put customer.txt /data/test/text

CREATE external TABLE customer(id int, name string, dob date, time1 timestamp) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/data/test/text';
select * from customer;

Create files and put them into external table that is partitioned

show databases;
show tables;
describe customer;

HIVE (hadoop/data/hive/warehouse)
Create managed Table
create table emp1(empId int, empName String, doj date);
INSERT OVERWRITE TABLE emp1 select * from emp (# this is non-partitioned table);
INSERT INTO TABLE emp1 select * from emp;
select * from emp1 where empId=1;
Drop Table emp1;

Create dynamic partitioned table and insert data into it from Non-partitioned table:-
Non-partitioned table --------- partitioned table
CREATE TABLE partition_date(column1 string) partitioned by (day string, event string)

CREATE TABLE non_partitioned_date1(column1 string, day string,event string); ( #data & data type)
insert into non_partitioned_date1 values('abc', '2000-01-01','e1') ( #data)  , ('abc', '2000-01-02','e1'), ('abc', '1999-12-20','e1') ,('abc', '2000-01-01','e1');

set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table partition_date partition(day,event) ( #data)  select column1,day,event ( #data)  from non_partitioned_date1;
show partitions partition_date;

Drop multiple partitions from table which have multiple partitions columns
//drop single partitioned table using single command
ALTER TABLE partition_date DROP PARTITION(day = ‘2000-01-01’, event = 'e1')

Create Buckets in hive
create table input_table (Street string,
City string,
Zip string,
State string,
Beds string,
Baths string,
Sq_feet int,
flat_type string,
Price int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE; (# row-based storage)

load data local inpath 'realstatewh.csv' into table input_table; (# you need directory for loading files)

SET hive.enforce.bucketing = true;
set hive.exec.dynamic.partition.mode=nonstrict;

create table bucket_table(Street string,
Zip string,
State string,
Beds string,
Baths string,
Sq_feet int,
flat_type string,
Price int) partitioned by(city string)( #data and data type) clustered by (street) )( #data) into 4 buckets ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


Insert values from managed table into bucketed table
Non-partitioned table (#managed table)  partitioned table (#managed table)
insert into table bucket_table partition(city) select street,zip,state,beds,baths,sq_feet,flat_type,price,city from input_table;


Insert values into partitioned table directly
create table employee(id bigint, name string,age int, salary bigint) partitioned by (department string);
insert into table employee partition(department='HR') values(1,'aarti',28,55000),(2,'shakshi',22,60000),(3,'mahesh',25,25000);
insert into table employee partition(department='BIGDATA') values(10001,'rajesh',29,50000),(10002,'rahul',23,250000),(10003,'dinesh',35,70000);
select * from employee order by id;

Convert text table to parquet table
create table employee_parquet(id bigint, name string,age int, salary bigint) STORED AS PARQUET;

Managed Table
•	Partitioned table
o	insert into table employee partition(department='HR') values(1,'aarti',28,55000),(2,'shakshi',22,60000),(3,'mahesh',25,25000);

•	Non-partitioned table
o	INSERT OVERWRITE TABLE emp1 select * from emp (#non-partition -> non-partition)
o	Insert into non_partitioned_date1 values('abc', '2000-01-01','e1') ( #data)  , ('abc', '2000-01-02','e1'), ('abc', '1999-12-20','e1') ,('abc', '2000-01-01','e1');
o	load data local inpath 'realstatewh.csv' into table input_table;

•	Non- partitioned table  Partitioned table
o	SET hive.exec.dynamic.partition = true;
o	SET hive.exec.dynamic.partition.mode = nonstrict;
o	SET hive.exec.max.dynamic.partitions = 1000;
o	SET hive.exec.max.dynamic.partitions.pernode = 1000;
o	insert overwrite table partition_date partition(day,event) ( #data)  select column1,day,event ( #data)  from non_partitioned_date1;

External Table
•	Partitioned table
o	Hadoop fs -mkdir -p /data/test/static
o	hadoop fs -mkdir -p /data/test/static/year=2010
o	hadoop fs -mkdir -p /data/test/static/year=2011
o	hadoop fs -put file1 /data/test/static/year=2010
o	hadoop fs -put file11 /data/test/static/year=2011
o	
o	Open hive console if it is not already opened - run command 

o	set hive.mapred.mode = strict;


o	create external table userdata(userId string, userName String) partitioned by (year string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/data/test/static' ;
o	ALTER TABLE userdata ADD PARTITION (year = '2010') LOCATION '/data/test/static/year=2010';
o	ALTER TABLE userdata ADD PARTITION (year = '2011') LOCATION '/data/test/static/year=2011';
o	select * from userdata where year=2011;

•	Non-partitioned table
o	nano customer.txt
o	ctrl + s
o	ctrl + x
o	hadoop fs -mkdir -p /data/test/text
o	hadoop fs -put customer.txt /data/test/text
o	CREATE external TABLE customer(id int, name string, dob date, time1 timestamp) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/data/test/text';
o	select * from customer;



Create Array Table in hive
create table tab7 (id int,name string,sal bigint,sub array<string>,city string)
row format delimited fields terminated by ','  collection items terminated by '$';
Create Map Table in hive

create table tab10(id int,name string,sal bigint,sub array<string>,dud map<string,int>,city string)
row format delimited fields terminated by ',' collection items terminated by '$' map keys terminated by '#';

Create Struct Table in hive
create table tab11 (id int,name string,sal bigint,sub array<string>,dud map<string,int>,addr struct<city:string,state:string,pin:bigint>) row format delimited fields terminated by ',' collection items terminated by '$' map keys terminated by '#';
```

 



 
## SPARK
```
from pyspark.sql import SparkSession
spark:SparkSession = SparkSession.builder.master("local[1]").appName("bootcamp.com".getOrCreate()

Create RDD
data = [1,2,3,4,5,6,7,8,9,10,11,12]
rdd=spark.sparkContext.parallelize(data)
rdd.count()
Create RDD using file
rdd2 = spark.sparkContext.textFile("file:///home/takeo/customer.txt")
rdd2.count()

Creating empty RDD with partition
rdd=spark.sparkContext.parallelize(data, 10)

Repartition and Coalesce
reparRdd = rdd10.repartition(4) (#total 4 partition)
coaleRdd = rdd10.coalesce(4) (#each machine 4 partition)

RDD Operations
rdd2 = rdd.flatMap(lambda x: x.split(" "))
rdd3 = rdd2.map(lambda x: (x,1))
rdd5 = rdd3.reduceByKey(lambda a,b: a+b)
rdd6 = rdd5.map(lambda x: (x[1],x[0])).sortByKey() (# this is ascending by default)
rdd4 = rdd3.filter(lambda x : 'an' in x[0])
firstRec = rdd6.first() (# this return first record)
datMax = rdd6.max()
totalWordCount = rdd6.reduce(lambda a,b: (a[0]+b[0],a[1]))
data3 = rdd6.take(3)
data = rdd6.collect()
saveAsTextFile()
rdd6.saveAsTextFile("file:///home/takeo/wordCount")
Create DataFrame from RDD using toDF()

columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

rdd = spark.sparkContext.parallelize(data)
df = rdd.toDF(columns)
df.printSchema()
df.show()


PySpark StructType & StructField
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

data = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]
schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])
df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show()

Defining Nested StructType object struct
data = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
  ]
schema = StructType([
        StructField('name', StructType([
             				StructField('firstname', StringType(), True),
             				StructField('middlename', StringType(), True),
             				StructField('lastname', StringType(), True)
             ])),
         StructField('id', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])

df2 = spark.createDataFrame(data=data,schema=schema)
df2.printSchema()
df2.show(truncate=False)

Ways to create DF
•	rdd = spark.sparkSession.parallelize(data)
•	df = rdd.toDF(columns)

ONE SHOT
•	df = spark.createDataFrame(data=data,schema=schema)

•	df = spark.createDataFrame(data).toDF(columns)
```
## SPARK FUNCTIONS
```
select()
df.select("firstname","lastname").show()
df.select(df.firstname,df.lastname).show()
df.select(df["firstname"],df["lastname"]).show()

By using col() function
from pyspark.sql.functions import col
df.select(col("firstname"),col("lastname")).show()

Select All
df.select("*").show()

Select Columns by Index
df.select(df.columns[0:3]).show(3) 
0:3 – show 3 columns
3 – show 3 rows

Select Nested Struct Columns from PySpark
df2.select("name").show(truncate=False)
df2.select("name.firstname","name.lastname").show(truncate=False)

withColumn()
ddf = df.withColumn("salary",col("salary").cast("Double")) (#change data type)

Update The Value of an Existing Column
udf = df.withColumn("salary",col("salary")*100)

Add a New Column using withColumn() with constant value
df.withColumn("Country", lit("USA")).show()

Rename Column Name
df.withColumnRenamed("gender","sex").show(truncate=False) 

DataFrame filter() with Column Condition
df.filter(df.state == "OH").show(truncate=False)
# Not equals condition
df.filter(df.state != "OH").show(truncate=False)
df.filter(~(df.state == "OH")).show(truncate=False)

DataFrame filter() with SQL Expression
df.filter("gender == 'M'").show()

For not equal
df.filter("gender != 'M'").show()
df.filter("gender <> 'M'").show()

PySpark Filter with Multiple Conditions
df.filter( (df.state  == "OH") & (df.gender  == "M") ).show()  general expression
OR,
df.filter(“state == ‘OH’ AND gender == ‘M’”).show() SQL expression

Filter Based on List Values
li=["OH","CA","DE"]
df.filter(df.state.isin(li)).show()
df.filter(~df.state.isin(li)).show()

Filter Based on Starts With, Ends With, Contains
df.filter(df.state.startswith("N")).show()
df.filter(df.state.endswith("H")).show()
df.filter(df.state.contains("H")).show()

Filter like and ilike
df2.filter(df2.name.like("%rose%")).show() 
df2.filter(df2.name.ilike("%rose%")).show()

Filter on an Array column
df.filter(array_contains(df.languages,"Java")).show()     

Filtering on Nested Struct columns
df.filter(df.name.lastname == "Williams").show() 

distinct()
dropDuplicates()
Get Distinct Rows (By Comparing All Columns)
distinctDF = df.distinct()
df2 = df.dropDuplicates()

Distinct of Selected Multiple Columns
dropDisDF = df.dropDuplicates(["department","salary"])

orderBy() and sort()
df.sort("department","state").show(truncate=False)
df.orderBy("department","state").show(truncate=False)

df.sort(df.department.asc(),df.state.desc()).show(truncate=False)
```








