'use strict';

var utils = require('../utils/writer.js');
var Argone = require('../service/ArgoneService');

module.exports.findargone = function findargone (req, res, next, id, forecastOrigin, forecastGeolocation, metadata, compression, data, batchmeta, page) {
  Argone.findargone(id, forecastOrigin, forecastGeolocation, metadata, compression, data, batchmeta, page)
    .then(function (response) {
      utils.writeJson(res, response);
    })
    .catch(function (response) {
      utils.writeJson(res, response);
    });
};

module.exports.findargoneMeta = function findargoneMeta (req, res, next, id, page) {
  Argone.findargoneMeta(id, page)
    .then(function (response) {
      utils.writeJson(res, response);
    })
    .catch(function (response) {
      utils.writeJson(res, response);
    });
};
