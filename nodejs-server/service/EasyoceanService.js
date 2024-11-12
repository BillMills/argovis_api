'use strict';


/**
 * List all possible values for certain easyocean query string parameters
 *
 * parameter String Easyocean query string parameter to summarize possible values of.
 * returns List
 **/
exports.easyoceanVocab = function(parameter) {
  return new Promise(function(resolve, reject) {
    var examples = {};
    examples['application/json'] = [ "", "" ];
    if (Object.keys(examples).length > 0) {
      resolve(examples[Object.keys(examples)[0]]);
    } else {
      resolve();
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
exports.findeasyocean = function(id,startDate,endDate,polygon,box,center,radius,metadata,woceline,compression,data,presRange,verticalRange,batchmeta,section_start_date) {
  return new Promise(function(resolve, reject) {
    var examples = {};
    examples['application/json'] = [ { }, { } ];
    if (Object.keys(examples).length > 0) {
      resolve(examples[Object.keys(examples)[0]]);
    } else {
      resolve();
    }
  });
}


/**
 * Easyocean metadata search and filter.
 *
 * woceline String WOCE line to search for. See /cchdo/vocabulary?parameter=woceline for list of options. (optional)
 * returns List
 **/
exports.findeasyoceanmeta = function(woceline) {
  return new Promise(function(resolve, reject) {
    var examples = {};
    examples['application/json'] = [ {
  "occupancies" : [ {
    "varying_direction" : "varying_direction",
    "expocodes" : [ "expocodes", "expocodes" ],
    "static_direction" : "static_direction",
    "time_boundaries" : [ "2000-01-23T04:56:07.000+00:00", "2000-01-23T04:56:07.000+00:00" ]
  }, {
    "varying_direction" : "varying_direction",
    "expocodes" : [ "expocodes", "expocodes" ],
    "static_direction" : "static_direction",
    "time_boundaries" : [ "2000-01-23T04:56:07.000+00:00", "2000-01-23T04:56:07.000+00:00" ]
  } ],
  "_id" : "_id",
  "date_updated_argovis" : "2000-01-23T04:56:07.000+00:00"
}, {
  "occupancies" : [ {
    "varying_direction" : "varying_direction",
    "expocodes" : [ "expocodes", "expocodes" ],
    "static_direction" : "static_direction",
    "time_boundaries" : [ "2000-01-23T04:56:07.000+00:00", "2000-01-23T04:56:07.000+00:00" ]
  }, {
    "varying_direction" : "varying_direction",
    "expocodes" : [ "expocodes", "expocodes" ],
    "static_direction" : "static_direction",
    "time_boundaries" : [ "2000-01-23T04:56:07.000+00:00", "2000-01-23T04:56:07.000+00:00" ]
  } ],
  "_id" : "_id",
  "date_updated_argovis" : "2000-01-23T04:56:07.000+00:00"
} ];
    if (Object.keys(examples).length > 0) {
      resolve(examples[Object.keys(examples)[0]]);
    } else {
      resolve();
    }
  });
}

