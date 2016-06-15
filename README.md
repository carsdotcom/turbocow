# TurboCow

This is a library to help with common data validation and enrichment activities in Spark.

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