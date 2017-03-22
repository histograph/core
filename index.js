const argv = require('minimist')(process.argv.slice(2));
const config = require('histograph-config');
const schemas = require('histograph-schemas');
const fuzzyDates = require('fuzzy-dates');
const Graphmalizer = require('graphmalizer-core');
const H = require('highland');
const Redis = require('redis');
const redisClient = Redis.createClient(config.redis.port, config.redis.host);
const u = require('util');
const defaultMapping = require('histograph-config/ESconfig')();

// Convert any ID, URI, URN to Histograph URN
const normalize = require('histograph-uri-normalizer').normalize;


const my_log = new log("core");

const elasticsearch = require('elasticsearch');
var esClient = new elasticsearch.Client({
  host: config.elasticsearch.host + ':' + config.elasticsearch.port
});

// Create a stream from Redis queue
var redis = H(function redisGenerator(push, next) {

  my_log.debug("In function redis");

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
  my_log.debug("Out function redis");
});

const ACTION_MAP = {
  add: 'add',
  update: 'add',
  delete: 'remove'
};

function getUnixTime(date) {
  return new Date(date).getTime() / 1000;
}

function toGraphmalizer(msg) {
  my_log.debug("In function toGraphmalizer: " + JSON.stringify(msg));

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

    // Add timestamp
    // TODO: find more structured way to add extra values/fields
    //   to Graphmalizer (and Neo4j afterwards) - API needs to remove
    //   those fields later
    d.validSinceTimestamp = getUnixTime(d.validSince[0]);
  }

  if (d.validUntil) {
    d.validUntil = fuzzyDates.convert(d.validUntil);

    // Add timestamp
    d.validUntilTimestamp = getUnixTime(d.validUntil[1]);
  }

  const ret = {
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

  my_log.debug("Almost Out function toGraphmalizer: " + JSON.stringify(ret));

  return ret;
}

// when passed an object, every field that contains an object
// is converted into a JSON-string (*mutates in place*)
function stringifyObjectFields(obj) {
  // convert objects to JSONified strings
  var d = JSON.parse(JSON.stringify(obj));

  if (typeof (d) === 'object') {
    Object.keys(d).forEach(function(k) {
      var v = d[k];

      if ( v !== null) {
        if ( v.constructor && v.constructor === Object) {
          d[k] = JSON.stringify(v);
        }
      }else{
        my_log.warn("Object with null value for key: " + k + ", values: " + JSON.stringify(d));
      }
    });
  }

  return d;
}

// Index into elasticsearch
const OP_MAP = {
  add: "index",
  remove: "delete"
};

// index documents into elasticsearch
function toElastic(data) {

  my_log.debug("In function toElastic");

  // select appropriate ES operation
  const operation = OP_MAP[data.operation];

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

  my_log.debug("Almost Out function toElastic: " + JSON.stringify(thing));
  return thing;
}

function logError(err) {
  my_log.error(err.stack || err);
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
var createIndex = H.wrapCallback(esClient.indices.create.bind(esClient.indices));

// find all indices in ES bulk request
function collectIndices(bulk_request)
{
  //client.cat.indices([params, [callback]])
  return H(bulk_request)
    // we only create indices for `index` events (not deletes)
    .filter(H.get('index'))

    // take out the elasticsearch index
    .map(H.get('index'))
    .map(H.get('_index'))

    // restrict to unique items
    .uniq()
    .reject(function (x) {
      // my_log.debug("I got: " + x);
      return esClient.indices.exists(x);
    });
}

function batchIntoElasticsearch(err, x, push, next){

  my_log.debug("In function batchIntoElasticsearch");

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
      my_log.debug("Creating index: " + indexName);
      // turn name into options for `esClient.indices.create`
      return {
        index: indexName,
        body: defaultMapping
      };

    })
    .map(createIndex)
    .parallel(10)
  	.errors(function(err){

      if(err && /index_already_exists_exception/.test(err.message)) {
        my_log.warn("Index already exists: " + err);
      } else {
        my_log.error("Failed creating index");
        my_log.error(err.message);
      }
    })

    // collect all results
    .sequence()
    .collect()
    .each(function(results){

      // tell it
      if (results && results.length > 0){
          my_log.debug("Created all indices: " + JSON.stringify(results));
      }else{
          my_log.debug("No index created");
      }

      // my_log.debug("Data: " + JSON.stringify(x));

      // bulk index index into elasticsearch
      esClient.bulk({body: x, requestTimeout: config.elasticsearch.requestTimeoutMs},
        function(err, resp){
        // ES oopsed, we send the error downstream
        if(err) {
          push(err);
          next();
          return;
        }

        // all went fine, send the respons downstream
        push(null, H([resp]));
        next();
      });

    });
    my_log.debug("Out function batchIntoElasticsearch");
}

function flatten(arrays) {
  return [].concat.apply([], arrays);
}

graphmalizer.register(commands)
    // we only index PiTs into elasticsearch, not relations.
    .map(function(d){
      return d.request.parameters;
    })
    .filter(function(d){
      return d.structure === 'node';
    })
    // first batch them up
    .batchWithTimeOrCount(argv.batchTimeout || config.core.batchTimeout, argv.batchSize || config.core.batchSize)

    // convert batch into batch of ES commands
    .map(function(pits) {
      // turn batch into a list of form [action, doc, action, doc, ...]
      return flatten(pits.map(toElastic));
    })
    .consume(batchIntoElasticsearch)
    .series()
    .errors(logError)
    .each(function(resp) {
      var r = resp || {took: 0, errors:false, items: []};
      my_log.debug("ES resp: " + JSON.stringify(resp));
      my_log.info("ES => " + r.items.length + " indexed, took " + r.took + "ms, errors: " + r.errors);
    });

my_log.info(config.logo.join('\n'));
