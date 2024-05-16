'use strict';


/**
 * Summarizes some float-level statistics for Argo BGC floats.
 *
 * returns inline_response_200_2
 **/
exports.argoBGC = function() {
  return new Promise(function(resolve, reject) {
    var examples = {};
    examples['application/json'] = {
  "summary" : [ {
    "_id" : "_id",
    "n" : 0.8008281904610115,
    "mostrecent" : "2010-01-01T00:00:00Z"
  }, {
    "_id" : "_id",
    "n" : 0.8008281904610115,
    "mostrecent" : "2010-01-01T00:00:00Z"
  } ],
  "_id" : "argo_bgc"
};
    if (Object.keys(examples).length > 0) {
      resolve(examples[Object.keys(examples)[0]]);
    } else {
      resolve();
    }
  });
}


/**
 * Summarizes some datacenter-level statistics about Argo data.
 *
 * returns inline_response_200_1
 **/
exports.argoDACs = function() {
  return new Promise(function(resolve, reject) {
    var examples = {};
    examples['application/json'] = {
  "summary" : [ {
    "_id" : "_id",
    "n" : 0.8008281904610115,
    "mostrecent" : "2010-01-01T00:00:00Z"
  }, {
    "_id" : "_id",
    "n" : 0.8008281904610115,
    "mostrecent" : "2010-01-01T00:00:00Z"
  } ],
  "_id" : "argo_dacs"
};
    if (Object.keys(examples).length > 0) {
      resolve(examples[Object.keys(examples)[0]]);
    } else {
      resolve();
    }
  });
}


/**
 * Summarizes some collection-level statistics about Argo data.
 *
 * returns inline_response_200
 **/
exports.argoOverview = function() {
  return new Promise(function(resolve, reject) {
    var examples = {};
    examples['application/json'] = {
  "summary" : {
    "datacenters" : [ "datacenters", "datacenters" ],
    "nCore" : 0.8008281904610115,
    "nBGC" : 6.027456183070403,
    "nDeep" : 1.4658129805029452,
    "mostrecent" : "2010-01-01T00:00:00Z"
  },
  "_id" : "argo_overview"
};
    if (Object.keys(examples).length > 0) {
      resolve(examples[Object.keys(examples)[0]]);
    } else {
      resolve();
    }
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
 * Argo search and filter.
 *
 * id String Unique ID to search for. (optional)
 * startDate Date ISO 8601 UTC date-time formatted string indicating the beginning of the time period of interest. (optional)
 * endDate Date ISO 8601 UTC date-time formatted string indicating the end of the time period of interest. (optional)
 * polygon String array of [lon, lat] vertices describing a polygon bounding the region of interest; final point must match initial point (optional)
 * box String lon, lat pairs of the lower left and upper right corners of a box on a mercator projection, packed like [[lower left lon, lower left lat],[upper right lon, upper right lat]] (optional)
 * winding String Enforce ccw winding for polygon (optional)
 * center List center to measure max radius from when defining circular region of interest; must be used in conjunction with query string parameter 'radius'. (optional)
 * radius BigDecimal km from centerpoint when defining circular region of interest; must be used in conjunction with query string parameter 'center'. (optional)
 * metadata String metadata pointer (optional)
 * platform String Unique platform ID to search for. (optional)
 * platform_type String Make/model of platform (optional)
 * positionqc List Argo position qc flag. (optional)
 * source List Experimental program source(s) to search for; document must match all sources to be returned. Accepts ~ negation to filter out documents. See /<data route>/vocabulary?parameter=source for list of options. (optional)
 * compression String Data minification strategy to apply. (optional)
 * mostrecent BigDecimal get back only the n records with the most recent values of timestamp. (optional)
 * data argo_data_keys Keys of data to include. Return only documents that have all data requested, within the pressure range if specified. Accepts ~ negation to filter out documents including the specified data. Omission of this parameter will result in metadata only responses. (optional)
 * presRange List Pressure range in dbar to filter for; levels outside this range will not be returned. (optional)
 * batchmeta String return the metadata documents corresponding to a temporospatial data search (optional)
 * returns List
 **/
exports.findArgo = function(id,startDate,endDate,polygon,box,winding,center,radius,metadata,platform,platform_type,positionqc,source,compression,mostrecent,data,presRange,batchmeta) {
  return new Promise(function(resolve, reject) {
    var examples = {};
    examples['application/json'] = [ {
  "metadata" : [ "metadata", "metadata" ],
  "geolocation_argoqc" : 1.4658129805029452,
  "data" : [ [ "", "" ], [ "", "" ] ],
  "basin" : 0.8008281904610115,
  "source" : [ {
    "date_updated" : "2000-01-23T04:56:07.000+00:00",
    "source" : [ "source", "source" ],
    "url" : "url",
    "doi" : "doi"
  }, {
    "date_updated" : "2000-01-23T04:56:07.000+00:00",
    "source" : [ "source", "source" ],
    "url" : "url",
    "doi" : "doi"
  } ],
  "date_updated_argovis" : "2000-01-23T04:56:07.000+00:00",
  "data_info" : [ "", "" ],
  "data_warning" : [ "degenerate_levels", "degenerate_levels" ],
  "vertical_sampling_scheme" : "vertical_sampling_scheme",
  "cycle_number" : 6.027456183070403,
  "timestamp_argoqc" : 5.962133916683182,
  "_id" : "_id",
  "profile_direction" : "profile_direction",
  "geolocation" : {
    "coordinates" : [ 0.8008281904610115, 0.8008281904610115 ],
    "type" : "type"
  },
  "timestamp" : "2000-01-23T04:56:07.000+00:00"
}, {
  "metadata" : [ "metadata", "metadata" ],
  "geolocation_argoqc" : 1.4658129805029452,
  "data" : [ [ "", "" ], [ "", "" ] ],
  "basin" : 0.8008281904610115,
  "source" : [ {
    "date_updated" : "2000-01-23T04:56:07.000+00:00",
    "source" : [ "source", "source" ],
    "url" : "url",
    "doi" : "doi"
  }, {
    "date_updated" : "2000-01-23T04:56:07.000+00:00",
    "source" : [ "source", "source" ],
    "url" : "url",
    "doi" : "doi"
  } ],
  "date_updated_argovis" : "2000-01-23T04:56:07.000+00:00",
  "data_info" : [ "", "" ],
  "data_warning" : [ "degenerate_levels", "degenerate_levels" ],
  "vertical_sampling_scheme" : "vertical_sampling_scheme",
  "cycle_number" : 6.027456183070403,
  "timestamp_argoqc" : 5.962133916683182,
  "_id" : "_id",
  "profile_direction" : "profile_direction",
  "geolocation" : {
    "coordinates" : [ 0.8008281904610115, 0.8008281904610115 ],
    "type" : "type"
  },
  "timestamp" : "2000-01-23T04:56:07.000+00:00"
} ];
    if (Object.keys(examples).length > 0) {
      resolve(examples[Object.keys(examples)[0]]);
    } else {
      resolve();
    }
  });
}


/**
 * Argo metadata search and filter.
 *
 * id String Unique ID to search for. (optional)
 * platform String Unique platform ID to search for. (optional)
 * returns List
 **/
exports.findArgometa = function(id,platform) {
  return new Promise(function(resolve, reject) {
    var examples = {};
    examples['application/json'] = [ {
  "country" : "country",
  "positioning_system" : "positioning_system",
  "platform_type" : "platform_type",
  "pi_name" : [ "pi_name", "pi_name" ],
  "wmo_inst_type" : "wmo_inst_type",
  "data_type" : "data_type",
  "data_center" : "data_center",
  "instrument" : "instrument",
  "_id" : "_id",
  "oceanops" : "oceanops",
  "fleetmonitoring" : "fleetmonitoring",
  "platform" : "platform"
}, {
  "country" : "country",
  "positioning_system" : "positioning_system",
  "platform_type" : "platform_type",
  "pi_name" : [ "pi_name", "pi_name" ],
  "wmo_inst_type" : "wmo_inst_type",
  "data_type" : "data_type",
  "data_center" : "data_center",
  "instrument" : "instrument",
  "_id" : "_id",
  "oceanops" : "oceanops",
  "fleetmonitoring" : "fleetmonitoring",
  "platform" : "platform"
} ];
    if (Object.keys(examples).length > 0) {
      resolve(examples[Object.keys(examples)[0]]);
    } else {
      resolve();
    }
  });
}

