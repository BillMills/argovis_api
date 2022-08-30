'use strict';
const argo = require('../models/argo');
const summaries = require('../models/summary');
const helpers = require('../helpers/helpers')
const geojsonArea = require('@mapbox/geojson-area');

/**
 * Summarizes some float-level statistics for Argo BGC floats.
 *
 * returns inline_response_200_3
 **/
exports.argoBGC = function() {
  return new Promise(function(resolve, reject) {
    const query = summaries.find({"_id":"argo_bgc"})
    query.exec(helpers.queryCallback.bind(null,null, resolve, reject))
  });
}


/**
 * Summarizes some datacenter-level statistics about Argo data.
 *
 * returns inline_response_200_2
 **/
exports.argoDACs = function() {
  return new Promise(function(resolve, reject) {
    const query = summaries.find({"_id":"argo_dacs"})
    query.exec(helpers.queryCallback.bind(null,null, resolve, reject))
  });
}


/**
 * Summarizes some collection-level statistics about Argo data.
 *
 * returns inline_response_200_1
 **/
exports.argoOverview = function() {
  return new Promise(function(resolve, reject) {
    const query = summaries.find({"_id":"argo_overview"})
    query.exec(helpers.queryCallback.bind(null,null, resolve, reject))
  });
}


/**
 * List all possible values for certain Argo query string parameters
 *
 * parameter String Argo query string parameter to summarize possible values of.
 * returns List
 **/
exports.argoVocab = function(parameter) {
  return new Promise(function(resolve, reject) {
    if(parameter == 'source'){
      resolve(['argo_core', 'argo_bgc', 'argo_deep'])
      return
    }
    if(parameter == 'data_keys'){
      const query = summaries.find({"_id":"argo_data_keys"}).lean()
      query.exec(helpers.queryCallback.bind(null,x=>x[0]['data_keys'], resolve, reject))
    }

    let lookup = {
        'platform': 'platform', // <parameter value> : <corresponding key in metadata document>
        'source': 'source.source'
    }

    argo['argoMeta'].find().distinct(lookup[parameter], function (err, vocab) {
      if (err){
        reject({"code": 500, "message": "Server error"});
        return;
      }
      resolve(vocab)
    })
  });
}


/**
 * Argo search and filter.
 *
 * id String Unique ID to search for. (optional)
 * startDate Date ISO 8601 UTC date-time formatted string indicating the beginning of the time period of interest. (optional)
 * endDate Date ISO 8601 UTC date-time formatted string indicating the end of the time period of interest. (optional)
 * polygon String array of [lon, lat] vertices describing a polygon bounding the region of interest; final point must match initial point (optional)
 * multipolygon String array of polygon regions; region of interest is taken as the intersection of all listed polygons. (optional)
 * center List center to measure max radius from when defining circular region of interest; must be used in conjunction with query string parameter 'radius'. (optional)
 * radius BigDecimal km from centerpoint when defining circular region of interest; must be used in conjunction with query string parameter 'center'. (optional)
 * platform String Unique platform ID to search for. (optional)
 * source List Experimental program source(s) to search for; document must match all sources to be returned. Accepts ~ negation to filter out documents. See /profiles/vocabulary?parameter=source for list of options. (optional)
 * compression String Data minification strategy to apply. (optional)
 * mostrecent BigDecimal get back only the n records with the most recent values of timestamp. (optional)
 * data argo_data_keys Keys of data to include. Return only documents that have all data requested, within the pressure range if specified. Accepts ~ negation to filter out documents including the specified data. Omission of this parameter will result in metadata only responses. (optional)
 * presRange List Pressure range in dbar to filter for; levels outside this range will not be returned. (optional)
 * returns List
 **/
exports.findArgo = function(res, id,startDate,endDate,polygon,multipolygon,center,radius,platform,source,compression,mostrecent,data,presRange) {
  return new Promise(function(resolve, reject) {
    // input sanitization
    let params = helpers.parameter_sanitization(id,startDate,endDate,polygon,multipolygon,center,radius)
    if(params.hasOwnProperty('code')){
      // error, return and bail out
      reject(params)
      return
    }

    // decide y/n whether to service this request
    if(source && ![id,(startDate && endDate),polygon,multipolygon,(center && radius),platform].some(x=>x)){
      reject({"code": 400, "message": "Please combine source queries with at least one of a time range, spatial extent, id or platform search."})
      return
    }
    let bailout = helpers.request_sanitation(params.polygon, null, params.center, params.radius, params.multipolygon) 
    if(bailout){
      reject(bailout)
      return
    }

    // local filter: fields in data collection other than geolocation and timestamp 
    let local_filter = []
    if(id){
        local_filter = [{$match:{'_id':id}}]
    }

    // optional source filtering
    if(source){
      local_filter.push(helpers.source_filter(source))
    }

    // postprocessing parameters
    let pp_params = {
        compression: compression,
        data: data,
        presRange: presRange,
        mostrecent: mostrecent
    }

    // metadata table filter: no-op promise if nothing to filter metadata for, custom search otherwise
    let metafilter = Promise.resolve([{_id: null}])
    let metacomplete = false
    if(platform){
        let match = {
            'platform': platform
        }

        metafilter = argo['argoMeta'].aggregate([{$match: match}]).exec()
        metacomplete = true
    }

    // datafilter must run syncronously after metafilter in case metadata info is the only search parameter for the data collection
    let datafilter = metafilter.then(helpers.datatable_stream.bind(null, argo['argo'], params, local_filter))

    Promise.all([metafilter, datafilter])
        .then(search_result => {

          let postprocess = helpers.post_xform(argo['argoMeta'], pp_params, search_result, res)

          resolve([search_result[1], postprocess])

        })

  });
}


/**
 * Argo metadata search and filter.
 *
 * id String Unique ID to search for. (optional)
 * platform String Unique platform ID to search for. (optional)
 * returns List
 **/
exports.findArgometa = function(res, id,platform) {
  return new Promise(function(resolve, reject) {
    let match = {
        '_id': id,
        'platform': platform
    }
    Object.keys(match).forEach((k) => match[k] === undefined && delete match[k]);

    const query = argo['argoMeta'].aggregate([{$match:match}]);
    let postprocess = helpers.meta_xform(res)
    resolve([query.cursor(), postprocess])
  });
}

