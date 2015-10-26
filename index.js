var argv = require('minimist')(process.argv.slice(2));
var config = require('histograph-config');
var schemas = require('histograph-schemas');
var fuzzyDates = require('fuzzy-dates');
var Graphmalizer = require('graphmalizer-core');
var H = require('highland');
var Redis = require('redis');
var redisClient = Redis.createClient(config.redis.port, config.redis.host);
var u = require('util');
var defaultMapping = require('./default-mapping')

// Convert any ID, URI, URN to Histograph URN
var normalize = require('histograph-uri-normalizer').normalize;

var elasticsearch = require('elasticsearch');
var esClient = new elasticsearch.Client({
  host: config.elasticsearch.host + ':' + config.elasticsearch.port
});

// Create a stream from Redis queue
var redis = H(function redisGenerator(push, next) {

  // Function called on each Redis message (or timeout)
  function redisCallback(err, data) {
    // handle error
    if (err) {
      push(err);
      next();
      return;
    }

    // attempt parse or error
    try {
      var d = JSON.parse(data[1]);
      push(null, d);
      next();
    }
    catch (e) {
      push(e);
      next();
    }
  }

  // blocking pull from Redis
  redisClient.blpop(config.redis.queue, 0, redisCallback);
});

var ACTION_MAP = {
  add: 'add',
  update: 'add',
  delete: 'remove'
};

function toGraphmalizer(msg) {
  function norm(x) {
    if (x) {
      return normalize(x, msg.dataset);
    }

    return undefined;
  }

  var d = msg.data || {};

  // dataset is a top-level attribute that we want copied into the 'data' attribute
  d.dataset = msg.dataset;

  // Parse fuzzy dates to arrays using fuzzy-dates module
  if (d.validSince) {
    d.validSince = fuzzyDates.convert(d.validSince);
  }

  if (d.validUntil) {
    d.validUntil = fuzzyDates.convert(d.validUntil);
  }

  return {
    operation: ACTION_MAP[msg.action],

    dataset: d.dataset,

    type: d.type,

    // nodes are identified with id's or URI's, we don't care
    id: norm(d.id || d.uri),

    // formalize source/target id's
    source: norm(d.from),
    target: norm(d.to),

    data: stringifyObjectFields(d)
  };
}

// when passed an object, every field that contains an object
// is converted into a JSON-string (*mutates in place*)
function stringifyObjectFields(obj) {
  // convert objects to JSONified strings
  var d = JSON.parse(JSON.stringify(obj));

  if (typeof (d) === 'object') {
    Object.keys(d).forEach(function(k) {
      var v = d[k];

      if (v.constructor === Object) {
        d[k] = JSON.stringify(v);
      }
    });
  }

  return d;
}

// Index into elasticsearch
var OP_MAP = {
  add: "index",
  remove: "delete"
};

// index documents into elasticsearch
function toElastic(data) {
  // select appropriate ES operation
  var operation = OP_MAP[data.operation];

  // replace string version with original
  if (data.data && data.data.geometry) {
    try {
      data.data.geometry = JSON.parse(data.data.geometry) ;
    } catch (_) {
    }
  }

  // { "action": { _index ... } , see
  // https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/api-reference-2-0.html
  var actionDesc = {};
  actionDesc[operation] = {
    _index: data.dataset,
    _type: data.type,
    _id: data.id
  };

  var thing = [actionDesc];

  // when removing no document is needed
  if(data.operation === "add")
    thing.push(data.data);

  return thing;
}

function logError(err) {
  console.error(err.stack || err);
}

var commands = redis
    .errors(logError)
    .map(toGraphmalizer);

var neo4jAuth;
if (config.neo4j.user && config.neo4j.password) {
  neo4jAuth = config.neo4j.user + ':' + config.neo4j.password;
}

var gconf = {
  types: schemas.graphmalizer,
  Neo4J: {
    hostname: config.neo4j.host,
    port: config.neo4j.port,
    auth: neo4jAuth
  },
  batchSize: argv.batchSize || config.core.batchSize,
  batchTimeout: argv.batchTimeout || config.core.batchTimeout
};

var graphmalizer = new Graphmalizer(gconf);


// create named index
var createIndex = H.wrapCallback(esClient.indices.create.bind(esClient.indices))

// find all indices in ES bulk request
function collectIndices(bulk_request)
{
  return H(bulk_request)
    // we only create indices for `index` events (not deletes)
    .filter(H.get('index'))

    // take out the elasticsearch index
    .map(H.get('index'))
    .map(H.get('_index'))

    // restrict to unique items
    .uniq()
}

function batchIntoElasticsearch(err, x, push, next){
  if (err) {
    // pass errors along the stream and consume next value
    push(err);
    next();
    return;
  }

  if (x === H.nil) {
    // pass nil (end event) along the stream,
    push(null, H.nil);
    // dont call next() on finished stream
    return;
  }

  // Create indices, then bulk index
  collectIndices(x)
    .map(function(indexName){
      // turn name into options for `esClient.indices.create`
      return {
        index: indexName,
        body: defaultMapping
      }
    })
    .map(createIndex)
    .series()
  	.errors(function(err){console.log("ignored", err && err.message)})

    // collect all results
    .collect()
    .each(function(results){

      // tell it
      console.log("Created all indices", results)

      // bulk index index into elasticsearch
      esClient.bulk({body: x}, function(err, resp){
        // ES oopsed, we send the error downstream
        if(err) {
          push(err);
          next();
          return;
        }

        // all went fine, send the respons downstream
        push(null, H([resp]));
        next();
      })

    });
}

function flatten(arrays) {
  return [].concat.apply([], arrays);
}

graphmalizer.register(commands)
    // we only index PiTs into elasticsearch, not relations.
    .map(function(d){
      return d.request.parameters
    })
    .filter(function(d){
      return d.structure === 'node';
    })
    // first batch them up
    .batchWithTimeOrCount(5000, 1000)

    // convert batch into batch of ES commands
    .map(function(pits) {
      // turn batch into a list of form [action, doc, action, doc, ...]
      return flatten(pits.map(toElastic));
    })
    .consume(batchIntoElasticsearch)
    .series()
    .errors(logError)
    .each(function(resp) {
      var r = resp || {took: 0, errors:false, items: []}
      console.log("ES => %d indexed, took %dms, errors: %s", r.items.length, r.took, r.errors)
    });

console.log(config.logo.join('\n'));
