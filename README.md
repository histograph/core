#Histograph-Core

Utility that parses NDJSON objects from Redis' `histograph-queue`, adds it to the Neo4j graph and performs inferencing.

##Preliminaries

- To build Histograph-Core, you need to install [Apache Maven](http://maven.apache.org/) first
- To run Histograph-Core, Redis server (`redis-server`) needs to be running

## Building Histograph-Core

Build the project by running `mvn clean install` in the `core` directory.

## Running Histograph-Core

Run the program by executing `bin/histograph-core.sh`.

##Input JSON syntax (through Redis)

JSON PIT object:

```
     {
       "action": ["add", "delete", "update"],
       "type": "pit",
		   "source": String,
       "data": {
			   "id": integer
			   "type": String
			   "name": String
		   }
     }
```
	
JSON Relation object:

```
     {
       "action": ["add", "delete"],
       "type": "relation",
		   "source": string,
       "data": {
	       "from": { 
	         "id": int,
	         [optional: "source": source]
	       },
	       "to": { 
	         "id": int,
	         [optional: "source": source]
	       },
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
