var argv = require('minimist')(process.argv.slice(2));
var config = require('histograph-config');
var schemas = require('histograph-schemas');
var fuzzyDates = require('fuzzy-dates');
var Graphmalizer = require('graphmalizer-core');
var H = require('highland');
var Redis = require('redis');
var redisClient = Redis.createClient(config.redis.port, config.redis.host);

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

  return {
    operation: ACTION_MAP[msg.action],

    dataset: d.dataset,

    // some old data uses `label` instead of `type`
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
      if (typeof (v) === 'object') {
        d[k] = JSON.stringify(v);
      }
    });
  }

  return d;
}

// Index into elasticsearch

// calling c.fn without 2nd argument yields a promise
var index = esClient.index.bind(esClient);
var remove = esClient.delete.bind(esClient);

var OP_MAP = {
  add: index,
  remove: remove
};

// index documents into elasticsearch
function toElastic(data) {
  // select appropriate ES operation
  var operation = OP_MAP[data.operation];

  // Normalize fuzzy dates (if present)
  if (data.data.validSince) {
    data.data.validSince = fuzzyDates.convert(data.data.validSince);
  }

  if (data.data.validUntil) {
    data.data.validUntil = fuzzyDates.convert(data.data.validUntil);
  }

  // replace string version with original
  try {
    data.data.geometry = JSON.parse(data.data.geometry) ;
  } catch (_) {
  }

  var opts = {
    index: data.dataset,
    type: data.type,
    id: data.id,
    body: data.data
  };

  // run it, returns a stream
  return H(operation(opts));
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

graphmalizer.register(commands)
    .map(function(d) {
      // only index nodes into elasticsearch
      if (d.request.parameters.structure === 'node') {
        return toElastic(d.request.parameters);
      }

      return H([]);
    })
    .series()
    .errors(logError)
    .each(function() {
      process.stdout.write('.');
    });

console.log(config.logo.join('\n'));
