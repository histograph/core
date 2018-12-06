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
	
  if( this.counter%4 == 0 ){
		
		console.log("Returning FALSE, counter is: " + this.counter + ", chunk is: " + chunk);


		// var state = this._writableState;

		// var last = state.lastBufferedRequest;
		// state.lastBufferedRequest = new WriteReq(chunk, enc, state.bufferedRequest.callback);

		// if (last) {
		// 		last.next = state.lastBufferedRequest;
		// } else {
		// 	state.bufferedRequest = state.lastBufferedRequest;
		// }
		// state.bufferedRequestCount += 1;

		// state.pendingcb += 1;

		// // state.length = 0;
		// if( state.bufferedRequest != null){
		// 	// state.length += 1;
		// 	var ptr = state.bufferedRequest.next;
		// 	while (ptr != null){
		// 		// state.length += 1;
		// 		console.log("Ptr: " + JSON.stringify(ptr));
		// 		ptr = ptr.next;
		// 	}
		// }
		
		setTimeout(this._write.bind(this),10000,chunk, enc, next);
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
	
	// var entry = self._writableState.bufferedRequest;
	// console.log("Consuming buffer: " + JSON.stringify(entry) );
	// while (entry) {
  //     var chunk = entry.chunk;
  //     var encoding = entry.encoding;
  //     var cb = entry.callback;

  //     self._write(chunk, encoding, cb);
  //     entry = entry.next;
  // }
	console.log("I will callback");
	next();
}

// This does not work, empty the buffer and decrease the highwater 
// seems to have to be done all manually
MyWriteStream.prototype._emit = function(self,next){
	
	var entry = self._writableState.bufferedRequest;
	console.log("Consuming buffer: " + JSON.stringify(entry) );

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

var consFunc = function(err,x,push,next){
	console.log("In consFunc is: " + JSON.stringify(x));
  if (err) {
            // pass errors along the stream and consume next value
    push(err);
    next();
  }else if (x === _.nil) {
    console.log("In consFunc got null");
    // pass nil (end event) along the stream
    push(null, x);
  }else{
  	https.get('https://api.histograph.io/search?q=burlutto', (res) => {
  	  console.log('statusCode:', res.statusCode);
  	  // console.log('headers:', res.headers);

  	  res.on('data', (d) => {
  			console.log("data called: " + d )
  	    // process.stdout.write(d);
        push(null,d);
  			next();
  	  });

  	}).on('error', (e) => {
  		console.log("error called")
  	  console.error(e);
      push(e,null);
      next();
  	}).on('end',() => {
  		console.log("end called")
      push(null, _.nil);
  	});
  }
	console.log("Out consFunc is: " + JSON.stringify(x));
}

function WriteReq(chunk, encoding, cb) { 
  this.chunk = chunk; 
  this.encoding = encoding; 
  this.callback = cb; 
  this.next = null; 
}

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



