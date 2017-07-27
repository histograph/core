const elasticsearch = require('elasticsearch');
var esClient = new elasticsearch.Client({host: 'localhost:9200'});

_=require('highland');



var simplePrintFunction = function(x){console.log("simplePrintFunction is: " + JSON.stringify(x));return x;};

_([1,2,3,4]).map(function(x){
	console.log("In ES is: " + x);
	esClient.count(function (error, response, status) {
	  if(error){
			console.log("maledizioneee" + error);
		}
	  console.log("In CB is: " + x);
	});
	console.log("Out ES is: " + x);
	return x;
}).map(simplePrintFunction).done(function(){console.log("done");});

// _([1,2,3,4]).sequence().map(simplePrintFunction).done(function(){console.log("done");});

var ESfunction = function(x,callback){
	console.log("In ES is: " + x);
	esClient.info(callback);
	console.log("Out ES is: " + x);
};

var wrappedfunction = _.wrapCallback(ESfunction);



// wrappedfunction =  _.wrapCallback(esClient.info.bind(esClient));

_([1,2,3,4]).map(simplePrintFunction).map(wrappedfunction).sequence().map(simplePrintFunction).done(function(x){console.log("done");});

_([1,2,3,4]).map(wrappedfunction).each(simplePrintFunction).done(function(x){console.log("done: " + JSON.stringify(x));});

_([1,2,3,4]).map(wrappedfunction).parallel(4).each(simplePrintFunction).done(function(){console.log("done");});

_([1,2,3,4]).map(wrappedfunction).flatten().each(simplePrintFunction).done(function(x){console.log("done");});

_([1,2,3,4]).map(wrappedfunction).done(function(x){console.log("done");});

_([1,2,3,4]).map(wrappedfunction).parallel(2).done(function(){console.log("done");});