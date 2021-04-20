<p align="center">
  <img src="jetfuel-logo.png">
</p>

## Overview
JetFuel is a utility designed to optimize [Hive](https://cwiki.apache.org/confluence/display/Hive/Home) tables to improve performance and reduce cost.
When invoked, it creates a data-identical copy of a Hive table, but with potentially different storage characteristics, including file format, compression and number/size of files.

These optimizations have been tuned to deliver good performance out-of-the-box, but everything can be configured as needed.

JetFuel is designed to work specifically on Hive tables, and requires a JDBC connection to [HiveServer2](https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Overview).

JetFuel has been tested to build and run on the latest versions of OpenJDK 8.  It has not been tested on newer versions and may be incompatible.

### Features

#### File Format Change
JetFuel can change tables from one data format to another. Supported formats for both source & target tables include:
* ORC
* PARQUET
* AVRO (without schema files)
* RC
* SEQ
* TEXT

#### Compression
JetFuel can apply compression to certain data formats, as well as re-compress into a different codec. Supported formats include:
* SNAPPY - for all file formats
* ZLIB - for ORC file format only
* GZIP - for PARQUET, SEQ, RC, TEXT
* UNCOMPRESSED - No compression is performed. Valid for all file formats

Table representing valid combinations of file formats and compression settings

||SNAPPY|ZLIB|GZIP|UNCOMPRESSED|
|:--------------------|:----:|:----:|:---:|:---:|
|`PARQUET`|Yes|No| Yes | Yes  |
|`ORC`|Yes|Yes|No| Yes  |
|`PARQUET`|Yes|No| Yes | Yes  |
|`AVRO`|Yes|No| No | Yes  |
|`SEQ`|Yes|No| Yes | Yes  |
|`RC`|Yes|No| Yes | Yes  |
|`TEXT`|Yes|No| Yes | Yes  |

#### Compaction
JetFuel automatically compacts small files into larger files, generally improving query performance.  Compaction options are
available to control the size of files JetFuel outputs.

## Documentation
This README is intended to provide detailed technical documentation for advanced users.

## General Operation

Below is a high level summary of the steps that JetFuel performs:

1. Read configuration YAML file
2. Open HiveServer2 JDBC connection
3. Configure various Hive properties for the current session
4. Execute Hive queries to create target Hive table
5. Execute Hive queries to copy all data from the source table into the target table

## Building JetFuel

### Prerequisites

JetFuel is a Java project using Maven as the build system.  

Building JetFuel requires the following:
* JDK 8
* Maven
* rpmbuild (only to build the RPM)

In MacOS, rpmbuild can be installed via Brew:

```bash
brew update && brew install rpm
```

## Building

Clone the project using the following command
```bash
git clone git@github.com:ExpediaGroup/jetfuel.git
```

Build and package the JetFuel JAR and RPM:
```bash
mvn clean verify
```

Optionally, an RPM can be built which contains the JetFuel JAR.  Enable the RPM profile to include this:
```bash
mvn clean verify -P rpm
```

Both the JAR and the RPM (if built) will appear in the `target/` directory.

### IDE Usage

JetFuel can be imported into your IDE of choice, however it depends on [Lombok](https://projectlombok.org/): Project Lombok is a java library that automatically plugs into your editor and build tools, spicing up your java

The Lombok plugin for your IDE must be installed for compilation to work successfully.

## Installation 

JetFuel can be installed via the RPM or by manually deploying the JAR file into the target server.  JetFuel does not need to run directly on a Hadoop cluster since it relies on JDBC connections. 

```bash
sudo yum install jetfuel
```

By default, the RPM installs JetFuel to `/opt/jetfuel/lib`.

## Run

Run JetFuel from a shell:

```bash
java -jar /opt/jetfuel/lib/jetfuel.jar -yamlFile fileName.yml
```

By default, JetFuel writes logs out to console.  Internally it uses [Log4j](https://logging.apache.org/log4j/1.2/manual.html), and the built-in configuration can be replaced by including 
a new configuration file on the classpath: 

```bash
java -Dlog4j.configuration=file:"./customLog4j.properties" -jar /opt/jetfuel/lib/jetfuel.jar -yamlFile fileName.yml
```

### JetFuel YAML Configuration Reference
The table below describes all the available configuration values for JetFuel:

|Property|Required|Description|Type|Example|
|:--------------------|:----:|:----:|:---:|:---:|
|`sourceDatabase`|Yes|Source database name| String | test  |
|`sourceTable`|Yes|Source table name| String | clickstream  |
|`targetDatabase`|Yes|Target database name| String | jetfuel  |
|`targetTable`|Yes|Target table name| String | clickstream_parquet  |
|`targetFileFormat`|Yes|Target file format| String | parquet  |
|`targetCompression`|NO|Target compression| String | snappy  |
|`targetCompaction`|Yes|Target compaction| Boolean | true. If not required can put empty string  |
|`maxSplit`|NO|Min split size for hive queries| String | 1000000000  |
|`minSplit`|NO|Max split size for hive queries| String | 256000000  |
|`smallFileAvgSize`|NO|Small Files Avg Size| String | 1000000000  |
|`sizePerTask`|NO|Size per task| String | 1000000000  |
|`hiveMetastoreUri`|YES|Hive metastore uri| String | thrift://hostname:9083  |
|`hiveServer2Url`|YES|Hive Server2 Url| String | jdbc:hive2://hiveserver2:10001/default  |
|`hiveServer2Username`|YES|Hive Server2 username| String | username  |
|`hiveServer2Password`|YES|Hive Server2 password| String | password  |
|`partitionFilter`|NO|Partition filter for partitions to be jetfueled| String | (trans_month = '2012-09') OR (trans_month = '2014-04') OR (trans_month = '2014-07') OR (trans_month = '2015-03') |
|`enablePartitionGrouping`|NO|Enables partition grouping insert queries| boolean | true |
|`partitionGrouping`|NO|Configures partition grouping strategy. One of: NONE, STATIC, DYNAMIC | string | DYNAMIC |
|`insertPartitionGroupSize`|NO|Insert Partition Group Size to group partitions for insert queries| Long | 100  |
|`groupPartitionOverride`|NO|[DEPRECATED] Same as enablePartitionGrouping | boolean | true |
|`mapReduceMemoryInMB`|NO|Map reduce memory in mb| Long | 10240  |
|`mapReduceJavaOptsInMB`|NO|Map reduce java opts in mb| Long | 10240  |
|`parquetBlockSize`|NO|Parquet block size| Long | 67108864  |
|`parquetPageSize`|NO|Parquet page size| Long | 67108864  |
|`configQueries`|NO|Optional list of additional queries to run before insert queries| boolean | set mapred.map.tasks=985 |
|`preFueling.dropTable`|NO|When true would delete and recreate the target table before fueling. When false and target table exists would not drop and recreate the target table. When false and target table does not exist would drop and recreate the target table.| boolean |   preFueling.dropTarget: false |

### YAML Example

    ---
      sourceDatabase: "test"
      sourceTable: "clickstream"
      targetDatabase: "jetfuel"
      targetTable: "clickstream_parquet"
      targetFileFormat: "parquet"
      targetCompression: "snappy"
      targetCompaction: true
      maxSplit: 1000000000
      minSplit: 256000000
      smallFileAvgSize: 1000000000
      sizePerTask: 1000000000
      hiveMetastoreUri: "thrift://hostname:9083"
      hiveServer2Url: "jdbc:hive2://hiveserver2:10001/default"
      hiveServer2Username: "username"
      hiveServer2Password: "password"
      partitionFilter: "(trans_month = '2012-09') OR (trans_month = '2014-04') OR (trans_month = '2014-07') OR (trans_month = '2015-03')"
      enablePartitionGrouping: false
      insertPartitionGroupSize: 100
      partitionGrouping: dynamic
      mapReduceMemoryInMB: 10240
      mapReduceJavaOptsInMB: 10240
      parquetBlockSize: 67108864
      parquetPageSize: 67108864
      configQueries:
        - "set mapred.map.tasks=985"
        - "analyze table test.clickstream compute statistics"
      preFueling:
        dropTarget: false

### Usage Information

#### Partition Filter

By default, JetFuel will copy an entire table from source to target.  However, there is an optional `partitionFilter` value which 
limits the scope of JetFuel to certain partitions.  The syntax is identical to that of a Hive `WHERE` clause.

**Please note:** The Partition Grouping feature described below currently requires `partitionFilter` to be present and in the 
form `key = value OR key = value OR ...`.

#### Dynamic Partitioning

For partitioned tables, JetFuel makes use of Hive's [Dynamic Partitioning](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#LanguageManualDML-DynamicPartitionInserts) feature to efficiently copy all partitions.  It creates a single `INSERT` query to copy all partitions, which generally performs significantly better than separate queries per partition.

However, this performed poorly for Parquet tables during our testing.  As a result, we implemented a partition grouping
feature which is automatically enabled for Parquet tables.  More details below.

#### Partition Grouping

This optional feature copies partitions from the source to target table in groups, rather than one large query.  
It is automatically enabled for Parquet tables, but can be manually enabled using `enablePartitionGrouping` if needed.

There are two options that configure how Dynamic Partitioning works:

|Property|Required|Description|Type|Example|
|:--------------------|:----:|:----:|:---:|:---:|
|`enablePartitionGrouping`|NO|Enables partition grouping insert queries| boolean | true |
|`partitionGrouping`|NO|Configures partition grouping strategy. One of: NONE, STATIC, DYNAMIC | string | DYNAMIC |
|`insertPartitionGroupSize`|NO|Insert Partition Group Size to group partitions for insert queries| Long | 100  |

Setting `partitionGrouping` to `STATIC`/`static` will force JetFuel to group partitions in fixed-size groups, 
using the `insertPartitionGroupSize` option to determine the size of each group.  If any of the groups fails, 
JetFuel will fallback to running each partition independently.

On the other hand, dynamic partitioning will automatically adjust the size of the partition groups to maximize efficiency
and minimize failures. If any query failures occur, it will dynamically adjust and retry.  In this mode, `insertPartitionGroupSize` 
will be used as the initial group size for the dynamic algorithm. 

## Tests

Maven automatically runs unit tests while building JetFuel.  

Coverage information is available at `target/site/jacoco/index.html`  Using `mvn clean verify` will automatically check 
code coverage and fail the build if the coverage requirements are not met.

Currently, the integration test suite for JetFuel cannot be made publicly available, but we are working on getting them open sourced.

## Contributing

We gladly accept contributions to this project in the form of issues, feature requests, and pull requests! Please refer to [CONTRIBUTING.md](CONTRIBUTING.md) for more information.

## Licensing

This project is available under the Apache 2.0 License.

Copyright 2018-2021 Expedia, Inc.
