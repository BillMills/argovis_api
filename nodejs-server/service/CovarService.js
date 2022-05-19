'use strict';


/**
 * Probability distribution field for a float at point lat-lon after forcastDays.
 *
 * lat BigDecimal latitude (degrees) of Argo float location
 * lon BigDecimal longitude (degrees) of Argo float location
 * forcastDays BigDecimal number of days over which to project Argo float drift
 * returns CovarSchema
 **/
exports.findCovar = function(lat,lon,forcastDays) {
  return new Promise(function(resolve, reject) {
    var examples = {};
    examples['application/json'] = {
  "features" : [ {
    "geometry" : {
      "coordinates" : [ 0.8008281904610115, 0.8008281904610115 ],
      "type" : "type"
    },
    "type" : "type",
    "properties" : { }
  }, {
    "geometry" : {
      "coordinates" : [ 0.8008281904610115, 0.8008281904610115 ],
      "type" : "type"
    },
    "type" : "type",
    "properties" : { }
  } ],
  "dLat" : 6.027456183070403,
  "dLong" : 1.4658129805029452,
  "_id" : "_id",
  "forcastDays" : 5
};
    if (Object.keys(examples).length > 0) {
      resolve(examples[Object.keys(examples)[0]]);
    } else {
      resolve();
    }
  });
}


/**
 * Sum of probability distribution field over a region for a float starting at point lat-lon after forcastDays.
 *
 * lat BigDecimal latitude (degrees) of Argo float location
 * lon BigDecimal longitude (degrees) of Argo float location
 * forcastDays BigDecimal number of days over which to project Argo float drift
 * polygon String array of [lon, lat] vertices describing a polygon bounding the region of interest; final point must match initial point (optional)
 * returns inline_response_200
 **/
exports.sumCovar = function(lat,lon,forcastDays,polygon) {
  return new Promise(function(resolve, reject) {
    var examples = {};
    examples['application/json'] = {
  "sum" : 0
};
    if (Object.keys(examples).length > 0) {
      resolve(examples[Object.keys(examples)[0]]);
    } else {
      resolve();
    }
  });
}

