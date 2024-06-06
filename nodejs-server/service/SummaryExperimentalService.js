'use strict';
const summaries = require('../models/summary');
const helpers = require('../helpers/helpers')

/**
 * Fetch a document from the summary collection by ID.
 *
 * id String Unique ID to search for. (optional)
 * key String If set, return only the named key from the summary document (intended for getting back just jsonld, for example) (optional)
 * returns Object
 **/
exports.fetchSummary = function(id,key) {
  return new Promise(function(resolve, reject) {
    const query = summaries.find({"_id":id}).lean()
    query.exec(helpers.queryCallback.bind(null,null, resolve, reject))
  });
}