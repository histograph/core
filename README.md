# Histograph-Core

Utility that parses NDJSON objects from Redis' `histograph-queue`, adds it to Neo4j, indexes the data with Elasticsearch and performs inferencing.

## Preliminaries

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
  - PostgreSQL needs to be running (try [postgresql.app](http://postgresapp.com) if you are on a Mac).
    `CREATE ROLE "histograph" LOGIN`.

## Building Histograph-Core

Build the project by running `mvn clean install` in the `core` directory.

## Running Histograph-Core

- Run the program by executing `bin/histograph-core.sh`.
- Arguments:
  - `-v` or `-verbose`: Toggle verbose output (including messages left)
  - `-config <file>`: Run with a configuration file, structured like [this](https://github.com/histograph/config).

__NOTES__
- The argument `-config` is optional -- the `HISTOGRAPH_CONFIG` environment variable is read if it is omitted.
- No progress output is provided if the `-verbose` argument is omitted. Because of this, you will not be able to see whether data import has completed. Killing the program prematurely results in an inconsistent state between the Neo4j graph, the Elasticsearch index and the Redis queue. So if you need to be certain that the program is done, run the program with `-verbose`.

## Use cUrl to test Core's Traversal API

By default, the Histograph Core Traversal API is running on [http://localhost:13782/](http://localhost:13782/). The Traversal API accepts HTTP POST request with body `{hgids: [hgid1, hgid2, ...]}` on [http://localhost:13782/traversal](http://localhost:13782/traversal). You can use cUrl to test the Traversal API:

    curl -X POST http://localhost:13782/traversal -d '{"hgids": ["geonames-altnames/1616926"]}' | python -mjson.tool

## Cleaning up before / after running

- Run `bin/delete-index.sh` to remove the Elasticsearch index
- Run `rm -rf /tmp/histograph` to remove the (standard) Neo4j database path. TODO create script that uses path from `config` repo

## Java documentation

Javadocs for all Java classes are automatically generated while building to `doc/javadoc`.

##Input JSON syntax (through Redis)

JSON PIT object:

```
	{
		"action": ["add", "delete", "update"],
		"type": "pit",
		"source": String,
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
			"label": String
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
rpush "histograph-queue" "{'action': 'add', 'type': 'relation', 'source': 'graafje', 'data': { 'from': 123, 'to': 321, 'label': 'hg:absorbedBy' } }"
```

Delete PITs:

```
rpush "histograph-queue" "{'action': 'delete', 'type': 'pit', 'source': 'graafje', 'data': { 'id': '321', 'name': 'Bert', 'type': 'Human' } }"
```

Delete relations:

```
rpush "histograph-queue" "{'action': 'delete', 'type': 'relation', 'source': 'graafje', 'data': { 'from': 123, 'to': 321, 'label': 'hg:absorbedBy' } }"
```

Update PITs:

```
rpush "histograph-queue" "{'action': 'update', 'type': 'pit', 'source': 'graafje', 'data': { 'id': '321', 'name': 'Frits', 'type': 'Human' } }"
```

## License

The source for Histograph is released under the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
