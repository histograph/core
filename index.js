var argv = require('minimist')(process.argv.slice(2));

// any string is converted to URI
var normalizeIdentifiers = require('histograph-uri-normalizer').normalize;

var H = require('highland');
var graphmalizer = require('graphmalizer-core');

var config = require(process.env.HISTOGRAPH_CONFIG);

var Redis = require('redis');
var redis_client = Redis.createClient();
var queueName = argv.q || argv.queue || 'histograph-queue';

// create a stream from redis queue
var redis = H(function redisGenerator(push, next)
{
    // function called on each redis message (or timeout)
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

    // blocking pull from redis
    redis_client.blpop(queueName, 0, redisCallback);
});

var ACTION_MAP = {
    add: 'add',
    update: 'add',
    delete: 'remove'
}

function toGraphmalizer(msg)
{
    function norm(x)
    {
        if(x)
            return normalizeIdentifiers(x, msg.dataset);

        return undefined;
    }

    var d = msg.data;

    // move bunch of top level attributes into data
    // TODO read from JSON Schema?
    d.dataset = msg.dataset;
    d.name = d.name;
    d.geometry = d.geometry;
    d.hasBeginning = d.hasBeginning;
    d.hasEnd = d.hasEnd;

    return {
        operation: ACTION_MAP[msg.action],

        // some old data uses sourceId
        dataset: msg.dataset,

        // some old data uses `label` instead of `type`
        type: d.type,

        // nodes are identified with id's or URI's, we don't care
        id: norm(d.id || d.uri),

        // formalize source/target id's
        source: norm(d.from),
        target: norm(d.to),

        data: stringifyObjectFields(d)
    }
}

// when passed an object, every field that contains an object
// is converted into a JSON-string (*mutates in place*)
function stringifyObjectFields(obj){
    // convert objects to JSONified strings
    var d = JSON.parse(JSON.stringify(obj));
    if(typeof(d) === 'object')
        Object.keys(d).forEach(function(k){
            var v = d[k];
            if(typeof(v) === 'object')
                d[k] = JSON.stringify(v);
        });

    return d;
}

// index into elasticsearch
var ES = require('elasticsearch');
var es_client = ES.Client({host: 'localhost:9200'});

// calling c.fn without 2nd argument yields a promise
var index = es_client.index.bind(es_client);
var remove = es_client.delete.bind(es_client);

var OP_MAP = {
    add: index,
    remove: remove
};

// index documents into elasticsearch
function toElastic(data){
    // select appropriate ES operation
    var operation = OP_MAP[data.operation];

    // replace string version with original
    data.data.geometry = JSON.parse(data.data.geometry);

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

graphmalizer(commands)
    .map(function(d){
        // only index nodes into elasticsearch
        if(d.request.parameters.structure === 'node')
            return toElastic(d.request.parameters);

        return H([]);
    })
    .series()
    .errors(logError)
    .each(function(){
        process.stdout.write('.');
    });

console.log(config.logo.join('\n'));
