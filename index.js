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
const WritableBulkIndexing = require('./writableBulk');

// Convert any ID, URI, URN to Histograph URN
const normalize = require('histograph-uri-normalizer').normalize;

const log = require('histograph-logging');

const my_log = new log("core");

const elasticsearch = require('elasticsearch');
var esClient = new elasticsearch.Client({
  host: config.elasticsearch.host + ':' + config.elasticsearch.port
});

var bulk = new WritableBulkIndexing(esClient);

// Create a stream from Redis queue
var redis = H(function redisGenerator(push, next) {

  my_log.debug("In function redis");

  // Function called on each Redis message (or timeout)
  function redisCallback(err, data) {
    my_log.debug("In redisCallback");
    // handle error
    if (err) {
      push(err);
      next();
      return;
    }

    // attempt parse or error
    try {
      var d = JSON.parse(data[1]);
      my_log.debug("Read from redis: " + JSON.stringify(d));
      push(null, d);
      next();
    }
    catch (e) {
      my_log.warn("Error parsing redis data " + JSON.stringify(e));
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
    id: norm(d.uri || d.id),

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
    } catch (err) {
      my_log.error("Geometry " + data.data.geometry + " cannot be parsed, err: " + err);
      my_log.debug(err.stack || "No Stack Info");
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


var commands = redis
    .errors(function(err){
      my_log.error("Error processing redis, error message: " + err);
      my_log.debug(err.stack || "No Stack Info");
    })
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

const createAndCatch = function(bulk_request,callback){
  my_log.debug("Checking index in: " + JSON.stringify(bulk_request));
  H(bulk_request)
  // we only create indices for `index` events (not deletes)
  .filter(H.get('index'))

  // extract the elasticsearch index
  .map(H.get('index'))
  .map(H.get('_index'))

  // restrict to unique items
  .uniq()
  .toArray(function(indexNames){
    
    if(indexNames.length === null || indexNames.length == 0) {
      my_log.debug("No index to create");
      callback(null,bulk_request);
    
    }else{

      my_log.debug("Creating index: " + indexNames);
      // turn name into options for `esClient.indices.create`          
      var lastOne = indexNames.length;
      for (var cursor=0;cursor<indexNames.length;cursor++){
        
        my_log.debug("Going to call ES with: {index: " + indexNames[cursor] + ",body: " + defaultMapping +"}");

        esClient.indices.create({index: indexNames[cursor],body: defaultMapping},function(err, resp){
          lastOne = lastOne-1;
          my_log.debug("Callback called, still pending: " + lastOne);
          if(err) {
            if(err && /index_already_exists_exception/.test(err.message)) {
              // my_log.debug("Index already exists: " + err);
              
              if (lastOne == 0 ){
                callback(null,bulk_request);
              }
            } else {
              my_log.error("Failed creating index");
              my_log.error(err.message);
              if (lastOne == 0 ){
                callback(err,bulk_request);
              }
            }
          }else{
              my_log.info("Created index: " + indexNames[cursor]);
              if (lastOne == 0 ){
                callback(null,bulk_request);
              }
          }
        });
      }
    }
  });  
};
  
  
var wrappedCreate = H.wrapCallback(createAndCatch);

// find all indices in ES bulk request
// function createIndices(bulk_request)
// {
//   //client.cat.indices([params, [callback]])
//     my_log.debug("Checking index in: " + bulk_request);
//     H(bulk_request)
//     // we only create indices for `index` events (not deletes)
//     .filter(H.get('index'))
// 
//     // extract the elasticsearch index
//     .map(H.get('index'))
//     .map(H.get('_index'))
// 
//     // restrict to unique items
//     .uniq()
//     .map(wrappedCreate)
//     .sequence()
//     .done(function (){
//       my_log.debug("Done with all new indices");
//     });
//     
//     
// }

function flatten(arrays) {
  return [].concat.apply([], arrays);
}

graphmalizer.register(commands)
    // we only index PiTs into elasticsearch, not relations.
    .map(function(d){
      my_log.debug("Extracting parameters " + JSON.stringify(d.request.parameters));
      return d.request.parameters;
    })
    .filter(function(d){
      my_log.debug("Filtering for nodes " + d.structure);
      return d.structure === 'node';
    })
    // first batch them up
    .batchWithTimeOrCount(argv.batchTimeout || config.core.batchTimeout, argv.batchSize || config.core.batchSize)

    // convert batch into batch of ES commands
    .map(function(pits) {
      // turn batch into a list of form [action, doc, action, doc, ...]
      my_log.debug("Creating actions for ES from " + JSON.stringify(pits));
      return flatten(pits.map(toElastic));
    })
    .map(wrappedCreate)
    .sequence()
    .errors(function(err) {
      my_log.error("Error during core processing: " + JSON.stringify(err));
    })
    .pipe(bulk);

my_log.info("\n" + config.logo.join('\n'));
