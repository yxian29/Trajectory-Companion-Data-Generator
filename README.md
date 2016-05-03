# Trajectory Companion Data Generator
---
```
This data generator works along with [Trajectory-Companion-Finder](https://github.com/samsonxian/Trajectory-Companion-Finder)
```

Trajectory Companion Data generator is a data streaming producer which takes a subset of [GeoLife GPS Trajectories Dataset](https://research.microsoft.com/en-us/downloads/b16d359d-d164-469e-9fd4-daa38f2b2e13/) as input, use Apache Spark to order the dataset by timestamps, and ingest data into ApacheKafaka.

## Prerequisite
* Setup Spark ([See instruction](http://spark.apache.org/docs/latest/spark-standalone.html))
* Setup Kafka ([See instruction](http://kafka.apache.org/documentation.html))
* Install Maven ([See instruction](https://maven.apache.org/install.html))
* Install Git

## Build
The project is managed by Maven. So simply execute maven `package` command at the same directory where `pom.xml` is located to generate jar file.
```
$ mvn clean package
```
Note: To build a jar with all dependencies, add option `-Dbuild-with-dependencies`

## Configuration
Make a copy of `./conf/default.properties.template`, uncomment and fill the following configurations:

```
# Zookeeper
zookeeper.host=127.0.0.1                    // zookeeper hostname
zookeeper.port=2181                         // zookeeper port
zookeeper.connection.string=127.0.0.1:2181  // not used

# Kafka
kafka.hostname=localhost                    // kafka server hostname
kafka.port=9092                             // kafka server port
kafka.topic=mytopic                         // subscribed topic name
kafka.producer.messagerate=0                // send message rate (in millisecond)
```

## Running application

```
$ $SPARK_HOME/bin/spark-submit --class tcdatagen.apps.SparkDataGen [jar] <configfile> <inputfile> [debug]
```

