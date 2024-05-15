'use strict';
const apihits = require('../models/apihits');
var utils = require('../utils/writer.js');
var Argone = require('../service/ArgoneService');
var helpers = require('../helpers/helpers')

module.exports.findargone = function findargone (req, res, next, id, forecastOrigin, forecastGeolocation, metadata, compression, data, batchmeta) {

  apihits.apihits.create({metadata: req.openapi.openApiRoute, query: req.query, isWeb: req.headers.origin === 'https://argovis.colorado.edu'})

  Argone.findargone(res, id, forecastOrigin, forecastGeolocation, metadata, compression, data, batchmeta)
    .then(pipefittings => helpers.data_pipeline.bind(null, res, batchmeta)(pipefittings),
    function (response) {
      utils.writeJson(res, response, response.code);
    })
    .catch(function (response) {
      utils.writeJson(res, response);
    });
};

module.exports.findargoneMeta = function findargoneMeta (req, res, next, id) {

  apihits.apihits.create({metadata: req.openapi.openApiRoute, query: req.query, isWeb: req.headers.origin === 'https://argovis.colorado.edu'})
  
  Argone.findargoneMeta(res, id)
    .then(pipefittings => helpers.data_pipeline.bind(null, res, false)(pipefittings),
    function (response) {
      utils.writeJson(res, response, response.code);
    })
    .catch(function (response) {
      utils.writeJson(res, response);
    });
};