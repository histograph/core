const https = require('https');
const _=require('highland');

const Writable = require('stream').Writable;



/**
 * @param counter useless initialization
 */
function MyWriteStream(counter) {
  if (!(this instanceof MyWriteStream)) {
    return new MyWriteStream(counter);
  }
  Writable.call(this, {objectMode:true,highWaterMark:counter});
  
	// this.highWaterMark = counter
  this.counter = 0;
	
	this.on('finish', function() {
		console.log("finish called");
  }.bind(this));
}

MyWriteStream.prototype = Object.create( Writable.prototype, {constructor: {value: MyWriteStream}} );

/**
 * @param chunk a bulk request as json.
 */
 
MyWriteStream.prototype._write = function(chunk, enc, next) {
  
	this.counter = this.counter + 1;
	console.log("counter is: " + this.counter + ", chunk is: " + chunk);
	
  if( this.counter%9 == 0 ){
		console.log("Returning FALSE, counter is: " + this.counter + ", chunk is: " + chunk);
		setTimeout(this._cb,30000,this,next);
	 	return false;
	}
	
	if( this.counter%15 == 0 ){
		next(new Error('I am a big error'));
		return;
	}
	
	
	clFunc(chunk,next);
	// setTimeout(next,10000,null,null);
	
  return true;

};


MyWriteStream.prototype._cb = function(self,next){
	console.log("I will callback");
	next();
}

// This does not work, empty the buffer and decrease the highwater 
// seems to have to be done all manually
MyWriteStream.prototype._emit = function(self,next){
	
	var entry = self._writableState.bufferedRequest;
	while (entry) {
      var chunk = entry.chunk;
      var encoding = entry.encoding;
      var cb = entry.callback;

      self._write(chunk, encoding, cb);
      entry = entry.next;
  }
	console.log("I will emit");
	self.emit('drain');
}

MyWriteStream.prototype._final = function(callback){
	console.log("Final called");
	callback();
	
}

mystream = new MyWriteStream(5);

var simplePrintFunction = function(x){console.log("simplePrintFunction is: " + JSON.stringify(x));return x;};

var clFunc = function(x,callback){
	console.log("In clFunc is: " + x);
	https.get('https://api.histograph.io/search?q=burlutto', (res) => {
	  console.log('statusCode:', res.statusCode);
	  // console.log('headers:', res.headers);

	  res.on('data', (d) => {
			console.log("data called")
	    // process.stdout.write(d);
			callback();
	  });

	}).on('error', (e) => {
		console.log("error called")
	  console.error(e);
	}).on('end',() => {
		console.log("end called")
		callback();
	});
	console.log("Out clFunc is: " + x);
}

var wrappedfunction = _.wrapCallback(clFunc);




/*
** 
** FUNCTION CALLS
**
*/

_([1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]).map(simplePrintFunction).pipe(mystream);null



// _([1,2,3,4]).sequence().map(simplePrintFunction).done(function(){console.log("done");});



_([1,2,3,4]).map(simplePrintFunction).map(wrappedfunction).sequence().map(simplePrintFunction).done(function(x){console.log("done");});

_([1,2,3,4]).map(wrappedfunction).each(simplePrintFunction).done(function(x){console.log("done: " + JSON.stringify(x));});

_([1,2,3,4]).map(wrappedfunction).parallel(4).each(simplePrintFunction).done(function(){console.log("done");});

_([1,2,3,4]).map(wrappedfunction).flatten().each(simplePrintFunction).done(function(x){console.log("done");});

_([1,2,3,4]).map(wrappedfunction).done(function(x){console.log("done");});

_([1,2,3,4]).map(wrappedfunction).parallel(2).toArray(simplePrintFunction).done(function(){console.log("done");});

_([1,2,3,4]).map(wrappedfunction).parallel(2).toArray(simplePrintFunction).done(function(){console.log("done");});



