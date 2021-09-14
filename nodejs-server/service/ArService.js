'use strict';


/**
 * shapes representing atmospheric rivers at a given date-time
 *
 * date Date three hour increments starting at 2004-01-01T00:00:00 and ending at 2017-04-02T03:00:00
 * returns arShapeSchema
 **/
exports.findARbyDate = function(date) {
  return new Promise(function(resolve, reject) {
    var examples = {};
    examples['application/json'] = {
  "date" : "2000-01-23T04:56:07.000+00:00",
  "date_formateed" : "2000-01-23",
  "shapeId" : 0,
  "geoLocation" : {
    "coordinates" : [ [ 6.027456183070403, 6.027456183070403 ], [ 6.027456183070403, 6.027456183070403 ] ],
    "type" : "type"
  },
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
 * shapes representing atmospheric rivers with a given ID
 *
 * _id String ID of an atmospheric river object
 * returns arShapeSchema
 **/
exports.findARbyID = function(_id) {
  return new Promise(function(resolve, reject) {
    var examples = {};
    examples['application/json'] = {
  "date" : "2000-01-23T04:56:07.000+00:00",
  "date_formateed" : "2000-01-23",
  "shapeId" : 0,
  "geoLocation" : {
    "coordinates" : [ [ 6.027456183070403, 6.027456183070403 ], [ 6.027456183070403, 6.027456183070403 ] ],
    "type" : "type"
  },
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
 * one instance of a shape representing an atmospheric river
 *
 * returns arShapeSchema
 **/
exports.findOneAR = function() {
  return new Promise(function(resolve, reject) {
    var examples = {};
    examples['application/json'] = {
  "date" : "2000-01-23T04:56:07.000+00:00",
  "date_formateed" : "2000-01-23",
  "shapeId" : 0,
  "geoLocation" : {
    "coordinates" : [ [ 6.027456183070403, 6.027456183070403 ], [ 6.027456183070403, 6.027456183070403 ] ],
    "type" : "type"
  },
  "_id" : "_id"
};
    if (Object.keys(examples).length > 0) {
      resolve(examples[Object.keys(examples)[0]]);
    } else {
      resolve();
    }
  });
}

