# TurboCow

This is a library to help with common data validation and enrichment activities in Spark.

## Setup

To use this library in your SBT project, add the following line to your build.sbt:
```
"com.cars.bigdata" %% "turbocow" % "{VERSION}" 
```
... where {VERSION} is the version you wish to use.  See tags in this project to determine which one you want.  (git tag -n)

There is example code in the ExampleApp.scala file in this project, that you can use as a template to get started.

## Overview

The code flow is as follows:

  1. Read in the input files (JSON format) into an RDD.
  1. Process the JSON files using ActionEngine.processJsonRDD(), which returns ```RDD[Map[String, String]]```
    * This transformes the JSON into enriched & validated Map structures.  
  1. You can transform the RDD into a DataFrame using function convertEnrichedRDDToDataFrame().
  1. Further custom processing can occur on the DataFrame.
  1. When done, you can write out the dataframe to Avro using AvroOutputWriter.write().
    * Before writing, you may wish to set default values according to the avro schema, which can be done with function DataFrameUtil.setDefaultValues().
  1. AvroOutputWriter.writeEnrichedRDD() to writes out the dataframe in Avro format.

## Action Engine

### Actions

TODO

#### Lookup ("lookup")

Here's how lookup works:

The framework searches in the config file for every Lookup action, determines exactly what fields to cache from which tables, and creates an in-memory map of Row objects where the key is the index fields needed.  There are multiple maps, because we may need different keys, but they all point to the same Row objects (to save memory).   The cached tables are then broadcast to every spark executor and 'queried' when a Lookup action is performed.

In the "onPass" and "onFail" sections, you can run any action list.  You can even reject the record here (or even in OnPass) by adding a "reject" actionType (see below).

##### DataFrames

Note that the project now supports using DataFrames for processing.  For projects that started out using the ActionEngine and RDDs, there is a function that allows you to transform the enriched RDD into a DataFrame (convertEnrichedRDDToDataFrame()), and then you can process the enriched data as a DataFrame.  

#### Reject ("reject")

Rejection is a separate action, but it has ramifications in the framework beyond its immediate effect in the action chain.  When a reject action runs... TODO

## Testing

When testing, if you "`import test.SparkTestContext._`" you will get access to global spark, sql, and hive contexts to use in your tests.  Do not create new contexts.  This allows you to separate the tests into different files.  

## Publishing

1. Compile & test locally
1. Run integration tests (or manual tests on cluster)
1. Test
1. When you're sure it's ready, apply a tag.  (currently in the 0.X series)
1. Create the jar and pom files via 'sbt publish-local'.
1. Publish to artifactory via:

```
SCALAVER=2.10
VER=0.3
# pom:
curl -v -H "X-JFrog-Art-Api: $ARTIFACTORY_KEY" -T ./turbocow_$SCALAVER-$VER.pom "https://repository.cars.com/artifactory/cars-data-local/com/cars/bigdata/turbocow_2.10/0.3/turbocow_$SCALAVER-$VER.pom"
# jar:
curl -v -H "X-JFrog-Art-Api: $ARTIFACTORY_KEY" -T ./turbocow_$SCALAVER-$VER.jar "https://repository.cars.com/artifactory/cars-data-local/com/cars/bigdata/turbocow_2.10/0.3/turbocow_$SCALAVER-$VER.jar"
```