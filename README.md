#Histograph-Core

Utility that parses NDJSON objects from Redis' `histograph-queue`, adds it to the Neo4j graph and performs inferencing.

##Preliminaries

- To build Histograph-Core:
  - You'll need Java 7 or 8 (Download JDK [here](http://www.oracle.com/technetwork/java/javase/downloads/index.html))
  - You'll need [Apache Maven](http://maven.apache.org/)
  - Make sure `$JAVA_HOME` is set to JDK 7 or 8
    - OS X: `export JAVA_HOME=$(/usr/libexec/java_home -v 1.7)` (or `-v 1.8`)
    - Linux: `TODO`
  - Double check with `mvn -v` if Maven uses the correct Java version
- To run Histograph-Core, Redis server (`redis-server`) needs to be running

## Building Histograph-Core

Build the project by running `mvn clean install` in the `core` directory.

## Running Histograph-Core

- Run the program by executing `bin/histograph-core.sh`.
- Run the program with verbose output by executing `bin/histograph-core.sh -v`.

##Input JSON syntax (through Redis)

JSON PIT object:

```
	{
		"action": ["add", "delete", "update"],
		"type": "pit",
		"layer": String,
		"data":
			"id": integer,
			"type": String,
			"name": String,
			"uri": String, (optional)
			"startDate": xsd:date, (optional) 
			"endDate": xsd:date, (optional)
			"geometry": GeoJSON string, (optional)
			"data": { (optional)
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
  
Add vertices:

```
rpush "histograph-queue" "{'action': 'add', 'type': 'vertex', 'source': 'graafje', 'data': {'id': '123', 'name': 'Rutger', 'type': 'Human' } }"
rpush "histograph-queue" "{'action': 'add', 'type': 'vertex', 'source': 'graafje', 'data': {'id': '321', 'name': 'Bert', 'type': 'Human' } }"
```

Add edges:

```
rpush "histograph-queue" "{'action': 'add', 'type': 'edge', 'source': 'graafje', 'data': { 'from': 123, 'to': 321, 'type': 'hg:absorbedBy' } }"
```

Delete vertices:

```
rpush "histograph-queue" "{'action': 'delete', 'type': 'vertex', 'source': 'graafje', 'data': { 'id': '321', 'name': 'Bert', 'type': 'Human' } }"
```

Delete edges:

```
rpush "histograph-queue" "{'action': 'delete', 'type': 'edge', 'source': 'graafje', 'data': { 'from': 123, 'to': 321, 'type': 'knows' } }"
```

Update vertices:

```
rpush "histograph-queue" "{'action': 'update', 'type': 'vertex', 'source': 'graafje', 'data': { 'id': '321', 'name': 'Frits', 'type': 'Human' } }"
```
