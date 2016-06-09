# Ingestion Framework

Library of reusable modules for Spark that can process common types of incoming data.

## Actions

### Lookup

Here's how lookup works:

The framework searches in the config file for every Lookup action, determines exactly what fields to cache from which tables, and creates an in-memory map of Row objects where the key is the index fields needed.  There are multiple maps, because we may need different keys, but they all point to the same Row objects (to save memory).   The cached tables are then broadcast to every spark executor and 'queried' when a Lookup action is performed.

