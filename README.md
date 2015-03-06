#Histograph-Core

Utility that parses NDJSON objects from Redis' `histograph-queue`, adds it to Neo4j, indexes the data with Elasticsearch and performs inferencing.

##Preliminaries

- To build Histograph-Core:
  - You'll need Java 7 or 8 (Download JDK [here](http://www.oracle.com/technetwork/java/javase/downloads/index.html))
  - You'll need [Apache Maven](http://maven.apache.org/)
  - You'll need a Histograph [configuration file](https://github.com/histograph/config), with either an environment variable `HISTOGRAPH_CONFIG` pointing to it, or passed on as an argument
  - Make sure `$JAVA_HOME` is set to JDK 7 or 8
    - OS X: `export JAVA_HOME=$(/usr/libexec/java_home -v 1.7)` (or `-v 1.8`)
    - Linux: `TODO`
  - Double check with `mvn -v` if Maven uses the correct Java version
- To run Histograph-Core:
  - Redis server (`redis-server`) needs to be running
  - [Elasticsearch](http://elasticsearch.org) needs to be running

## Building Histograph-Core

Build the project by running `mvn clean install` in the `core` directory.

## Running Histograph-Core

- Run the program by executing `bin/histograph-core.sh`.
- Arguments:
  - `-v` or `-verbose`: Toggle verbose output (including messages left)
  - `-config <file>`: Run with a configuration file, structured like [this](https://github.com/histograph/config).

__NOTES:__
- The argument `-config` is optional -- the `HISTOGRAPH_CONFIG` environment variable is read if it is omitted.
- No progress output is provided if the `-verbose` argument is omitted. Because of this, you will not be able to see whether data import has completed. Killing the program prematurely results in an inconsistent state between the Neo4j graph, the Elasticsearch index and the Redis queue. So if you need to be certain that the program is done, run the program with `-verbose`.

## Cleaning up before / after running

- Run `bin/delete-index.sh` to remove the Elasticsearch index
- Run `rm -rf /tmp/histograph` to remove the (standard) Neo4j database path. TODO create script that uses path from `config` repo

##Input JSON syntax (through Redis)

JSON PIT object:

```
	{
		"action": ["add", "delete", "update"],
		"type": "pit",
		"layer": String,
		"target": ["graph", "es"], 			// Optional, omit to send to both
		"data":
			"id": integer,
			"type": String,
			"name": String,
			"uri": String, 					// Optional
			"startDate": xsd:date, 			// Optional 
			"endDate": xsd:date, 			// Optional
			"geometry": GeoJSON string, 	// Optional
			"data": { 						// Optional
				...additional keys/values
			}
		}
	}
```
	
JSON Relation object:

```
	{
		"action": ["add", "delete"],
		"type": "relation",
		"source": String,
		"data": {
			"from": [integer | String], (internal ID or hgID)
			"to": [integer | String], (internal ID or hgID)
			"type": String
		}
	}
```

## I/O examples through Redis
  
Add PITs:

```
rpush "histograph-queue" "{'action': 'add', 'type': 'pit', 'source': 'graafje', 'data': {'id': '123', 'name': 'Rutger', 'type': 'Human' } }"
rpush "histograph-queue" "{'action': 'add', 'type': 'pit', 'source': 'graafje', 'data': {'id': '321', 'name': 'Bert', 'type': 'Human' } }"
```

Add relations:

```
rpush "histograph-queue" "{'action': 'add', 'type': 'relation', 'source': 'graafje', 'data': { 'from': 123, 'to': 321, 'type': 'hg:absorbedBy' } }"
```

Delete PITs:

```
rpush "histograph-queue" "{'action': 'delete', 'type': 'pit', 'source': 'graafje', 'data': { 'id': '321', 'name': 'Bert', 'type': 'Human' } }"
```

Delete relations:

```
rpush "histograph-queue" "{'action': 'delete', 'type': 'relation', 'source': 'graafje', 'data': { 'from': 123, 'to': 321, 'type': 'knows' } }"
```

Update PITs:

```
rpush "histograph-queue" "{'action': 'update', 'type': 'pit', 'source': 'graafje', 'data': { 'id': '321', 'name': 'Frits', 'type': 'Human' } }"
```
