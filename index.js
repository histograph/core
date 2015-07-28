var argv = require('minimist')(process.argv.slice(2));

var H = require('highland');
//var u = require('./utils');
//var graphmalizer = require('graphmalizer-core');

// create a stream from redis queue

var Redis = require('redis');
var redis_client = Redis.createClient();
var queueName = argv.q || argv.queue || 'histograph-queue';

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

redis.errors(console.error).each(H.log);