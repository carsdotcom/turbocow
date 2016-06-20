# TurboCow

This is a library to help with common data validation and enrichment activities in Spark.

## Setup

To use this library in your SBT project, add the following line to your build.sbt:
```
"com.cars.bigdata" %% "turbocow" % "{VERSION}" 
```
... where {VERSION} is the version you wish to use.  See tags in this project to determine which one you want.  (git tag -n)

There is example code in the ExampleApp.scala file in this project, that you can use as a template to get started.


## Actions

TODO

### Lookup ("lookup")

Here's how lookup works:

The framework searches in the config file for every Lookup action, determines exactly what fields to cache from which tables, and creates an in-memory map of Row objects where the key is the index fields needed.  There are multiple maps, because we may need different keys, but they all point to the same Row objects (to save memory).   The cached tables are then broadcast to every spark executor and 'queried' when a Lookup action is performed.

In the "onPass" and "onFail" sections, you can run any action list.  You can even reject the record here (or even in OnPass) by adding a "reject" actionType (see below).

### Reject ("reject")

Rejection is a separate action, but it has ramifications in the framework beyond its immediate effect in the action chain.  When a reject action runs... TODO

### Publishing

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