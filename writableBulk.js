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
  
  return true;

};

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
        setTimeout(self._doBulkIndexing,config.elasticsearch.retryTime,chunk, enc, next);
      }else{
        my_log.error("Error processing ES commands, message: " + err + " response: " + JSON.stringify(resp || '{"message":"no response provided"}'));
        my_log.error(err.stack || "No Stack Info");
        self.emit('error', err);
      }
    }else{
      var r = resp || {took: 0, errors:false, items: []};
      if( r.items == null){
        r.items = [];
      }
      my_log.debug("ES resp: " + JSON.stringify(resp));
      
      if ( r.errors ){
        var newrequest = [];
        for (var es_error=0;es_error<r.items.length;es_error++){  
          if (r.items[es_error].index && r.items[es_error].index.error != null ){
            if ('es_rejected_execution_exception' == r.items[es_error].index.error.type ){
                my_log.error("ES Queue full:" + JSON.stringify(r.items[es_error].index));  
                // self.needsToWait = true;
                // newrequest.push();
            }else{
                my_log.error("ES response:" + JSON.stringify(r.items[es_error].index));  
            }
          }
        }
      }
      my_log.info("ES => " + r.items.length + " indexed, took " + r.took + "ms, errors: " + r.errors);
      if( self.needsToWait ){
          self.needsToWait = false;
          self.omit('drain');
      }
      next(err,resp);      
    }
  });
}