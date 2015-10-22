# Histograph-Core

Utility that parses NDJSON objects from Redis' `histograph-queue`, adds it to
Neo4j, indexes the data with Elasticsearch and performs inferencing.

## Running Histograph-Core

	npm i
	node index.js --config [cfg.json]

- Arguments:
  - `--config <file>`: Run with a configuration file, structured like [this](https://github.com/histograph/config).

__NOTES__
- The argument `--config` is optional -- the `HISTOGRAPH_CONFIG` environment variable is read if it is omitted.
- No progress output is provided if the `-verbose` argument is omitted. Because of this, you will not be able to see whether data import has completed. Killing the program prematurely results in an inconsistent state between the Neo4j graph, the Elasticsearch index and the Redis queue. So if you need to be certain that the program is done, run the program with `-verbose`.

## Cleaning up before / after running

- Run `curl -X DELETE localhost:9200/` to remove all Elasticsearch indexes (beware).
- Run `neo4j-shell -c "MATCH (n) OPTIONAL MATCH (n)-[e]-() DELETE e, n RETURN DISTINCT true;"` to remove all content of the graph (but keep the indices)
- You can also remove the Neo4J database all together, look in your config file where it is located

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


Copyright (C) 2015 [Waag Society](http://waag.org).
