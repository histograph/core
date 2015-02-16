#Histograph-Core

Utility that reads NDJSON objects from Redis' `histograph-queue`.

##Preliminaries

###Redis 

- Redis needs to be running 

###Downloading and building Tinkerpop

- Download [Tinkerpop 3.0.0.M7](https://github.com/tinkerpop/tinkerpop3/archive/3.0.0.M7.zip)
- Build by running `mvn clean install` (takes a while)

###Setup neo4j Gremlin Server

- Go to `gremlin-server` directory in Tinkerpop directory
- Install neo4j plugin by running `bin/gremlin-server.sh -i com.tinkerpop neo4j-gremlin 3.0.0.M7`

###Running neo4j Gremlin Server

- Go to `gremlin-server` directory in Tinkerpop directory
- Start server: `bin/gremlin-server.sh conf/gremlin-server-neo4j.yaml`

###(Optional) Connecting to server with Gremlin Console

- Go to `gremlin-console` directory in Tinkerpop directory
- `:install com.tinkerpop neo4j-gremlin 3.0.0.M7`
- `:plugin use tinkerpop.neo4j`
- `:remote connect tinkerpop.server conf/remote-objects.yaml`

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
lpush "histograph-queue" "{'action': 'add', 'type': 'vertex', 'source': 'graafje', 'data': {'id': '123', 'name': 'Rutger', 'type': 'Human' } }"
lpush "histograph-queue" "{'action': 'add', 'type': 'vertex', 'source': 'graafje', 'data': {'id': '321', 'name': 'Bert', 'type': 'Human' } }"
```

Add edges:

```
lpush "histograph-queue" "{'action': 'add', 'type': 'edge', 'source': 'graafje', 'data': { 'from': 123, 'to': 321, 'type': 'knows' } }"
```

Delete vertices:

```
lpush "histograph-queue" "{'action': 'delete', 'type': 'vertex', 'source': 'graafje', 'data': { 'id': '321', 'name': 'Bert', 'type': 'Human' } }"
```

Delete edges:

```
lpush "histograph-queue" "{'action': 'delete', 'type': 'edge', 'source': 'graafje', 'data': { 'from': 123, 'to': 321, 'type': 'knows' } }"
```

Update vertices:

```
lpush "histograph-queue" "{'action': 'update', 'type': 'vertex', 'source': 'graafje', 'data': { 'id': '321', 'name': 'Frits', 'type': 'Human' } }"
```