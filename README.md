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

On Fail, you can run any action.  If add a "reject" action, then the rejection string will be read from a RejectionReason instance shared with the parent Action, whose responsibility it is to set the RejectionReason.

### Reject ("reject")

Reject can have an optional config where a rejection reason can be specified in the JSON, for example:
```
    {
      "actionType": "reject",
      "config": { 
        "reason": "some reason text"
      }
    }
```

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