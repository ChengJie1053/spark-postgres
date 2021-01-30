# PostgreSQL & GreenPlum Data Source for Apache Spark [![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html) [![](https://tokei.rs/b1/github/yaooqinn/spark-greenplum)](https://github.com/yaooqinn/spark-greenplum) [![GitHub release](https://img.shields.io/github/release/yaooqinn/spark-greenplum.svg)](https://github.com/yaooqinn/spark-greenplum/releases) [![codecov](https://codecov.io/gh/yaooqinn/spark-greenplum/branch/master/graph/badge.svg)](https://codecov.io/gh/yaooqinn/spark-greenplum) [![Build Status](https://travis-ci.com/yaooqinn/spark-greenplum.svg?branch=master)](https://travis-ci.com/yaooqinn/spark-greenplum)[![HitCount](http://hits.dwyl.io/yaooqinn/spark-greenplum.svg)](http://hits.dwyl.io/yaooqinn/spark-greenplum)

A library for reading data from and transferring data to Greenplum databases with Apache Spark, for Spark SQL and DataFrames.

This library is **100x faster** than Apache Spark's JDBC DataSource while transferring data from Spark to Greenpum databases.

Also, this library is fully **transactional** .

## Try it now !

### CTAS
```genericsql
CREATE TABLE tbl
USING greenplum
options ( 
  url "jdbc:postgresql://greenplum:5432/",
  delimiter "\t",
  dbschema "gptest",
  dbtable "store_sales",
  user 'gptest',
  password 'test')
AS
 SELECT * FROM tpcds_100g.store_sales WHERE ss_sold_date_sk<=2451537 AND ss_sold_date_sk> 2451520;
```

### View & Insert

```genericsql
CREATE TEMPORARY TABLE tbl
USING greenplum
options ( 
  url "jdbc:postgresql://greenplum:5432/",
  delimiter "\t",
  dbschema "gptest",
  dbtable "store_sales",
  user 'gptest',
  password 'test')
  
INSERT INTO TABLE tbl SELECT * FROM tpcds_100g.store_sales WHERE ss_sold_date_sk<=2451537 AND ss_sold_date_sk> 2451520;

```

Please refer to [Spark SQL Guide - JDBC To Other Databases](http://spark.apache.org/docs/latest/sql-data-sources-jdbc.html) to learn more about the similar usage. 
