'use strict';


/**
 * Fetch a document from the summary collection by ID.
 *
 * id String Unique ID to search for. (optional)
 * key String If set, return only the named key from the summary document (intended for getting back just jsonld, for example) (optional)
 * returns Object
 **/
exports.fetchSummary = function(id,key) {
  return new Promise(function(resolve, reject) {
    var examples = {};
    examples['application/json'] = { };
    if (Object.keys(examples).length > 0) {
      resolve(examples[Object.keys(examples)[0]]);
    } else {
      resolve();
    }
  });
}

