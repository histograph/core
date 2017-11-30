/**
 * Expose a writeable stream and execute it as a set of bulk requests.
 */
'use strict';

const Writable = require('stream').Writable;
const config = require('histograph-config');
const log = require('histograph-logging');

const my_log = new log("core");


module.exports = WritableBulkIndexing;

/**
 * @param esClient the ES client to be used
 */
function WritableBulkIndexing(esClient) {
  if (!(this instanceof WritableBulkIndexing)) {
    return new WritableBulkIndexing(esClient);
  }
  Writable.call(this, {objectMode:true});
  
  this.esClient = esClient;
  
  this.needsToWait = false;

}

WritableBulkIndexing.prototype = Object.create( Writable.prototype, {constructor: {value: WritableBulkIndexing}} );

/**
 * @param chunk a bulk request as json.
 */
 
WritableBulkIndexing.prototype._write = function(chunk, enc, next) {
  
  if (this.needsToWait){
    my_log.info("Waiting for ES");
    return false;
  }
  
  this._doBulkIndexing(chunk, enc, next);
  
  my_log.debug("Return true from writable");
  return true;

};

WritableBulkIndexing.prototype._resetNeedsToWait = function (self,next,err,resp){
  self.needsToWait = false;
  next(err,resp);
}

WritableBulkIndexing.prototype._doBulkIndexing = function (chunk, enc, next){
  
  my_log.debug("Data to index is: " + JSON.stringify(chunk));
  
  var self = this;
  
  // bulk index index into elasticsearch
  self.esClient.bulk({body: chunk, requestTimeout: config.elasticsearch.requestTimeoutMs},
    function(err, resp){
    // error from ES, we handle it
    if(err) {
      if (/Request Timeout after/.test(err)){
        my_log.warn("Timeout, message: " + err + " response: " + JSON.stringify(resp || '{"message":"no response provided"}'));
        self.needsToWait = true;
      }else{
        my_log.error("Error processing ES commands, message: " + err + " response: " + JSON.stringify(resp || '{"message":"no response provided"}'));
        my_log.error(err.stack || "No Stack Info");
        // return the error
        next(err);
        return;
      }
    }else{
      my_log.debug("ES resp: " + JSON.stringify(resp));
      
      var r = resp || {took: 0, errors:false, items: []};
      if( r.items == null){
        r.items = [];
      }
      
      if ( r.errors ){
        var newrequest = [];
        for (var err_cnt=0;err_cnt<r.items.length;err_cnt++){
          my_log.error("ES response:" + JSON.stringify(r.items[err_cnt]));

          if (r.items[err_cnt].index && r.items[err_cnt].index.error != null ){
            if ('es_rejected_execution_exception' == r.items[err_cnt].index.error.type ){
                my_log.error("ES Queue full:" + JSON.stringify(r.items[err_cnt].index));  
                self.needsToWait = true;
                // we should resubmit failed requests, but it is probably better to try again with the dataset
                // and take care that the system does not get overloaded
                // newrequest.push();
            }
          }
        }
      }
      my_log.info("ES => " + r.items.length + " indexed, took " + r.took + "ms, errors: " + r.errors);  
    }
    if( self.needsToWait ){
      // Delay calling the callback to give some rest to the system
      my_log.debug("Wait for timeout " + config.elasticsearch.retryTime);
      setTimeout(self._resetNeedsToWait,config.elasticsearch.retryTime,self,next,err,resp);
    }else{
        my_log.debug("No need to wait for ES");
        next(err,resp);
    }    
  });
}