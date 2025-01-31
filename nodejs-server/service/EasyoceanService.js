'use strict';
const easyocean = require('../models/easyocean');
const helpers = require('../helpers/helpers')


/**
 * List all possible values for certain easyocean query string parameters
 *
 * parameter String Easyocean query string parameter to summarize possible values of.
 * returns List
 **/
exports.easyoceanVocab = function(parameter) {
  return new Promise(function(resolve, reject) {
    if(parameter == 'enum'){
      resolve(["woceline", "data", "metadata"])
      return
    } else {
      if(parameter == 'metadata' || parameter == 'woceline'){
        const query = easyocean['easyoceanMeta'].find({},{"_id": 1}).lean()
        query.exec(helpers.queryCallback.bind(null,(arr) => arr.map(obj => obj._id), resolve, reject))
      } else if(parameter == 'data'){
        resolve(["pressure","ctd_temperature","ctd_salinity","doxy","conservative_temperature","absolute_salinity"])
        return
      }
    }
  });
}

/**
 * Easyocean search and filter.
 *
 * id String Unique ID to search for. (optional)
 * startDate Date ISO 8601 UTC date-time formatted string indicating the beginning of the time period of interest. (optional)
 * endDate Date ISO 8601 UTC date-time formatted string indicating the end of the time period of interest. (optional)
 * polygon String array of [lon, lat] vertices describing a polygon bounding the region of interest; final point must match initial point (optional)
 * box String lon, lat pairs of the lower left and upper right corners of a box on a mercator projection, packed like [[lower left lon, lower left lat],[upper right lon, upper right lat]] (optional)
 * center List center to measure max radius from when defining circular region of interest; must be used in conjunction with query string parameter 'radius'. (optional)
 * radius BigDecimal km from centerpoint when defining circular region of interest; must be used in conjunction with query string parameter 'center'. (optional)
 * metadata String metadata pointer (optional)
 * woceline String WOCE line to search for. See /cchdo/vocabulary?parameter=woceline for list of options. (optional)
 * compression String Data minification strategy to apply. (optional)
 * data List Keys of data to include. Return only documents that have all data requested, within the pressure range if specified. Accepts ~ negation to filter out documents including the specified data. Omission of this parameter will result in metadata only responses. (optional)
 * presRange List DEPRICATED, please use verticalRange instead. Pressure range in dbar to filter for; levels outside this range will not be returned. (optional)
 * verticalRange List Vertical range to filter for in pressure or depth as appropriate for this dataset; levels outside this range will not be returned. (optional)
 * batchmeta String return the metadata documents corresponding to a temporospatial data search (optional)
 * section_start_date Date Start date of the section of interest; see metadata corresponding to the WOCE line of interest for valid options. (optional)
 * returns List
 **/
exports.findeasyocean = function(res,id,startDate,endDate,polygon,box,center,radius,metadata,woceline,compression,data,presRange,verticalRange,batchmeta,section_start_date) {
  return new Promise(function(resolve, reject) {

    // input sanitization
    let params = helpers.parameter_sanitization('easyocean',id,startDate,endDate,polygon,box,false,center,radius)
    if(params.hasOwnProperty('code')){
      // error, return and bail out
      reject(params)
      return
    }
    params.batchmeta = batchmeta
    params.compression = compression
    params.metacollection = 'easyoceanMeta'
    params.verticalRange = presRange || verticalRange
    if(data && data.join(',') !== 'except-data-values'){
      params.data_query = helpers.parse_data_qsp(data.join(','))

      if(!('pressure' in params.data_query[0]) && !('pressure' in params.data_query[2])){
        // pull pressure out of mongo by default
        params.data_query[2]['pressure'] = []
        params.coerced_pressure = true
      }
    }
    params.lookup_meta = batchmeta
    params.compression = compression
    params.batchmeta = batchmeta

    // decide y/n whether to service this request
    let bailout = helpers.request_sanitation(params.polygon, params.center, params.radius, params.box, false, presRange, verticalRange) 
    if(bailout){
      reject(bailout)
      return
    }

    // local filter: fields in data collection other than geolocation and timestamp 
    let local_filter = {$match:{}}
    if(id){
        local_filter['$match']['_id'] = id
    }
    if(metadata){
      local_filter['$match']['metadata'] = metadata
    }
    if(section_start_date){
      local_filter['$match']['section_start_date'] = new Date(section_start_date) 
    }
    if(Object.keys(local_filter['$match']).length > 0){
      local_filter = [local_filter]
    } else {
      local_filter = []
    }

    // can we afford to project data documents down to a subset in aggregation?
    if(compression=='minimal' && data==null && presRange==null && verticalRange==null){
      params.projection = ['_id', 'metadata', 'geolocation', 'timestamp', 'source']
    }

    // metadata table filter: no-op promise, nothing to filter easy ocean on in metadata atm
    let metafilter = Promise.resolve([])
    params.metafilter = false
    if(woceline){
        let match = {
            '_id': woceline,
        }

        metafilter = easyocean['easyoceanMeta'].aggregate([{$match: match}]).exec()
        params.metafilter = true
    }


    // datafilter must run syncronously after metafilter in case metadata info is the only search parameter for the data collection
    let datafilter = metafilter.then(helpers.datatable_stream.bind(null, easyocean['easyocean'], params, local_filter))

    Promise.all([metafilter, datafilter])
        .then(search_result => {
          
          let stub = function(data){
              // given a data document,
              // return the record that should be returned when the compression=minimal API flag is set
              // should be id, long, lat, timestamp, and then anything needed to group this point together with other points in interesting ways.
              
              let sourceset = new Set(data.source.map(x => x.source).flat())

              return [
                data['_id'], 
                data.geolocation.coordinates[0], 
                data.geolocation.coordinates[1], 
                data.timestamp,
                data['metadata']
              ]
          }

          let postprocess = helpers.post_xform(params, search_result, res, stub)
          res.status(404) // 404 by default
          resolve([search_result[1], postprocess])
        })
  });
}


/**
 * Easyocean metadata search and filter.
 *
 * woceline String WOCE line to search for. See /cchdo/vocabulary?parameter=woceline for list of options. (optional)
 * returns List
 **/
exports.findeasyoceanmeta = function(res, woceline) {
  return new Promise(function(resolve, reject) {
    let match = {
        '_id': woceline
    }
    Object.keys(match).forEach((k) => match[k] === undefined && delete match[k]);

    const query = easyocean['easyoceanMeta'].aggregate([{$match:match}]);
    let postprocess = helpers.meta_xform(res)
    res.status(404) // 404 by default
    resolve([query.cursor(), postprocess])
  });
}

