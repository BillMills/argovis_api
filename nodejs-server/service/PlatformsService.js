'use strict';


/**
 * Provides a list of all Argo platform IDs with BGC data.
 *
 * returns summary
 **/
exports.platformBGC = function() {
  return new Promise(function(resolve, reject) {
    var examples = {};
    examples['application/json'] = {
  "summary" : { },
  "_id" : "_id"
};
    if (Object.keys(examples).length > 0) {
      resolve(examples[Object.keys(examples)[0]]);
    } else {
      resolve();
    }
  });
}


/**
 * Provides a list of platforms with their most recent known report and position.
 *
 * platforms List List of platform IDs
 * returns List
 **/
exports.platformList = function(platforms) {
  return new Promise(function(resolve, reject) {
    var examples = {};
    examples['application/json'] = [ {
  "cycle_number" : 6,
  "most_recent_date" : "2000-01-23T04:56:07.000+00:00",
  "dac" : "dac",
  "platform_number" : "",
  "number_of_profiles" : 0,
  "_id" : "",
  "geolocation" : {
    "coordinates" : [ 0.8008281904610115, 0.8008281904610115 ],
    "type" : "type"
  }
}, {
  "cycle_number" : 6,
  "most_recent_date" : "2000-01-23T04:56:07.000+00:00",
  "dac" : "dac",
  "platform_number" : "",
  "number_of_profiles" : 0,
  "_id" : "",
  "geolocation" : {
    "coordinates" : [ 0.8008281904610115, 0.8008281904610115 ],
    "type" : "type"
  }
} ];
    if (Object.keys(examples).length > 0) {
      resolve(examples[Object.keys(examples)[0]]);
    } else {
      resolve();
    }
  });
}


/**
 * Provides metadata for a specified platform.
 *
 * platform platform platform ID
 * returns platformMeta
 **/
exports.platformMeta = function(platform) {
  return new Promise(function(resolve, reject) {
    var examples = {};
    examples['application/json'] = {
  "PI_NAME" : "PI_NAME",
  "most_recent_date" : "2000-01-23T04:56:07.000+00:00",
  "most_recent_date_added" : "2000-01-23T04:56:07.000+00:00",
  "dac" : "dac",
  "platform_number" : "",
  "number_of_profiles" : 0,
  "POSITIONING_SYSTEM" : "POSITIONING_SYSTEM",
  "_id" : ""
};
    if (Object.keys(examples).length > 0) {
      resolve(examples[Object.keys(examples)[0]]);
    } else {
      resolve();
    }
  });
}

