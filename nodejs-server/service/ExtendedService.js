'use strict';
const Extended = require('../models/extended');
const helpers = require('../helpers/helpers')
const GJV = require('geojson-validation');
const summaries = require('../models/summary');

/**
 * Vocab data for the named extended object.
 *
 * extendedName String 
 * parameter String categorical extended object search and filter parameters
 * returns List
 **/
exports.extendedVocab = function(extendedName,parameter) {
  return new Promise(function(resolve, reject) {
    if(parameter == 'enum'){
      resolve(["data"])
      return
    }

    if(parameter == 'data'){
      const query = Extended['extendedMeta'].find({"_id":extendedName}).lean()
      query.exec(helpers.queryCallback.bind(null,x=>x[0]['data_keys'], resolve, reject))
    }
  });
}


/**
 * Search and filter for extended object named in path
 *
 * extendedName String 
 * id String Unique ID to search for. (optional)
 * startDate Date ISO 8601 UTC date-time formatted string indicating the beginning of the time period of interest. (optional)
 * endDate Date ISO 8601 UTC date-time formatted string indicating the end of the time period of interest. (optional)
 * polygon String array of [lon, lat] vertices describing a polygon bounding the region of interest; final point must match initial point (optional)
 * multipolygon String array of polygon regions; region of interest is taken as the intersection of all listed polygons. (optional)
 * box String lon, lat pairs of the lower left and upper right corners of a box on a mercator projection, packed like [[lower left lon, lower left lat],[upper right lon, upper right lat]] (optional)
 * winding String Enforce ccw winding for polygon and multipolygon (optional)
 * center List center to measure max radius from when defining circular region of interest; must be used in conjunction with query string parameter 'radius'. (optional)
 * radius BigDecimal km from centerpoint when defining circular region of interest; must be used in conjunction with query string parameter 'center'. (optional)
 * compression String Data minification strategy to apply. (optional)
 * mostrecent BigDecimal get back only the n records with the most recent values of timestamp. (optional)
 * data List Keys of data to include. Return only documents that have all data requested, within the pressure range if specified. Accepts ~ negation to filter out documents including the specified data. Omission of this parameter will result in metadata only responses. (optional)
 * batchmeta String return the metadata documents corresponding to a temporospatial data search (optional)
 * returns List
 **/

exports.findExtended = function(res,id,startDate,endDate,polygon,multipolygon,box,winding,center,radius,compression,mostrecent,extendedName,batchmeta,data) {
  return new Promise(function(resolve, reject) {
    // input sanitization

    // extended objects must be geo-searched by $geoIntersects, which is only supported on 2dsphere indexes; 
    // therefore, requests for box regions must be coerced into approximately corresponding geodesic-edged polygons.
    if(box) {
      box = helpers.box_sanitation(box, false, true)[0] // if we're going to search it like a polygon, we dont need to split on the dateline
        if(box.hasOwnProperty('code')){
        // error, return and bail out
        return box
      }

      polygon = JSON.stringify(helpers.box2polygon(box[0], box[1]).coordinates[0])

      // always enforce winding for boxes on extended object searches
      winding=true
    }

    let params = helpers.parameter_sanitization(extendedName,id,startDate,endDate,polygon,multipolygon,null,winding,center,radius)

    if(params.hasOwnProperty('code')){
      // error, return and bail out
      reject(params)
      return
    }

    params.mostrecent = mostrecent
    params.extended = true // extended objects need a geointersects search instead of geowithin for polygons and multipolygons
    params.batchmeta = batchmeta
    params.compression = compression

    // decide y/n whether to service this request
    let bailout = helpers.request_sanitation(params.polygon, params.center, params.radius, params.multipolygon) 
    if(bailout){
      reject(bailout)
      return
    }

    // local filter: fields in data collection other than geolocation and timestamp 
    let local_filter = {$match:{}}
    if(id){
        local_filter['$match']['_id'] = id
    }
    if(Object.keys(local_filter['$match']).length > 0){
      local_filter = [local_filter]
    } else {
      local_filter = []
    }

    // postprocessing parameters
    let pp_params = {
        compression: compression,
        data: JSON.stringify(data) === '["except-data-values"]' ? null : data, // ie `data=except-data-values` is the same as just omitting the data qsp
        dateRange: [params.startDate, params.endDate],
        mostrecent: mostrecent,
        suppress_meta: compression=='minimal' || batchmeta,
        batchmeta : batchmeta
    }

    // can we afford to project data documents down to a subset in aggregation?
    let projection = null
    if(compression=='minimal'){
      projection = ['_id', 'metadata', 'timestamp', 'geolocation']
    }

    // metadata table filter: no-op promise stub, nothing to filter grid data docs on from metadata at the moment
    let metafilter = Promise.resolve([])

    // datafilter must run syncronously after metafilter in case metadata info is the only search parameter for the data collection
    let datafilter = metafilter.then(helpers.datatable_stream.bind(null, Extended[extendedName], params, local_filter, projection, null))

    let batchmetafilter = datafilter.then(helpers.metatable_stream.bind(null, pp_params.batchmeta, Extended['extendedMeta']))

    Promise.all([metafilter, datafilter, batchmetafilter])
        .then(search_result => {

          let stub = function(data, metadata){
              // given a data and corresponding metadata document,
              // return the record that should be returned when the compression=minimal API flag is set
              // should be id, long, lat, timestamp, and then anything needed to group this point together with other points in interesting ways.
              return [
                data['_id'], 
                data['timestamp'],
                data['geolocation'],
                data['metadata']
              ]
          }
          let postprocess = helpers.post_xform(Extended['extendedMeta'], pp_params, search_result, res, stub)

          res.status(404) // 404 by default
          if(pp_params.batchmeta){
            resolve([search_result[2], postprocess])
          } else {
            resolve([search_result[1], postprocess])
          }
        })
  });
}


/**
 * Metadata for extended objects by ID
 *
 * id String Unique ID to search for. (optional)
 * returns List
 **/
exports.findextendedMeta = function(res, id) {
  return new Promise(function(resolve, reject) {
    let match = {
      '_id': id
    }
    Object.keys(match).forEach((k) => match[k] === undefined && delete match[k]);

    const query = Extended['extendedMeta'].aggregate([{$match:match}]);
    let postprocess = helpers.meta_xform(res)
    res.status(404) // 404 by default
    resolve([query.cursor(), postprocess])  
  })
}
