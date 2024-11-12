'use strict';


/**
 * Tropical cyclone search and filter.
 *
 * id String Unique ID to search for. (optional)
 * startDate Date ISO 8601 UTC date-time formatted string indicating the beginning of the time period of interest. (optional)
 * endDate Date ISO 8601 UTC date-time formatted string indicating the end of the time period of interest. (optional)
 * polygon String array of [lon, lat] vertices describing a polygon bounding the region of interest; final point must match initial point (optional)
 * box String lon, lat pairs of the lower left and upper right corners of a box on a mercator projection, packed like [[lower left lon, lower left lat],[upper right lon, upper right lat]] (optional)
 * center List center to measure max radius from when defining circular region of interest; must be used in conjunction with query string parameter 'radius'. (optional)
 * radius BigDecimal km from centerpoint when defining circular region of interest; must be used in conjunction with query string parameter 'center'. (optional)
 * name String name of tropical cyclone (optional)
 * metadata String metadata pointer (optional)
 * compression String Data minification strategy to apply. (optional)
 * data List Keys of data to include. Return only documents that have all data requested, within the pressure range if specified. Accepts ~ negation to filter out documents including the specified data. Omission of this parameter will result in metadata only responses. (optional)
 * batchmeta String return the metadata documents corresponding to a temporospatial data search (optional)
 * returns List
 **/
exports.findTC = function(id,startDate,endDate,polygon,box,center,radius,name,metadata,compression,data,batchmeta) {
  return new Promise(function(resolve, reject) {
    var examples = {};
    examples['application/json'] = [ {
  "data_warning" : {
    "duplicate" : [ "duplicate", "duplicate" ]
  },
  "metadata" : [ "metadata", "metadata" ],
  "data" : [ [ "", "" ], [ "", "" ] ],
  "_id" : "_id",
  "basin" : 0.8008281904610115,
  "record_identifier" : "record_identifier",
  "class" : "class",
  "geolocation" : {
    "coordinates" : [ 0.8008281904610115, 0.8008281904610115 ],
    "type" : "type"
  },
  "timestamp" : "2000-01-23T04:56:07.000+00:00"
}, {
  "data_warning" : {
    "duplicate" : [ "duplicate", "duplicate" ]
  },
  "metadata" : [ "metadata", "metadata" ],
  "data" : [ [ "", "" ], [ "", "" ] ],
  "_id" : "_id",
  "basin" : 0.8008281904610115,
  "record_identifier" : "record_identifier",
  "class" : "class",
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
 * Tropical cyclone metadata search and filter.
 *
 * id String Unique ID to search for. (optional)
 * name String name of tropical cyclone (optional)
 * returns List
 **/
exports.findTCmeta = function(id,name) {
  return new Promise(function(resolve, reject) {
    var examples = {};
    examples['application/json'] = [ {
  "data_info" : [ "", "" ],
  "num" : 0.8008281904610115,
  "data_type" : "data_type",
  "name" : "name",
  "_id" : "_id",
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
  "date_updated_argovis" : "2000-01-23T04:56:07.000+00:00"
}, {
  "data_info" : [ "", "" ],
  "num" : 0.8008281904610115,
  "data_type" : "data_type",
  "name" : "name",
  "_id" : "_id",
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
  "date_updated_argovis" : "2000-01-23T04:56:07.000+00:00"
} ];
    if (Object.keys(examples).length > 0) {
      resolve(examples[Object.keys(examples)[0]]);
    } else {
      resolve();
    }
  });
}


/**
 * List all possible values for certain tc query string parameters
 *
 * parameter String TC query string parameter to summarize possible values of.
 * returns List
 **/
exports.tcVocab = function(parameter) {
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

