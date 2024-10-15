'use strict';
const apihits = require('../models/apihits');
var Argone = require('../service/ArgoneService');
var helpers = require('../helpers/helpers')

module.exports.findargone = function findargone (req, res, next, id, forecastOrigin, forecastGeolocation, metadata, compression, data, batchmeta, page) {

  apihits.apihits.create({metadata: req.openapi.openApiRoute, query: req.query, isWeb: req.headers.origin === 'https://argovis.colorado.edu', avhTelemetry: req.headers.hasOwnProperty('x-avh-telemetry') ? req.headers['x-avh-telemetry'] : null})

  Argone.findargone(res, id, forecastOrigin, forecastGeolocation, metadata, compression, data, batchmeta, page)
    .then(
      pipefittings => helpers.data_pipeline.bind(null, req, res, batchmeta)(pipefittings),
      helpers.lookupReject.bind(null, req, res)
    )
    .catch(helpers.catchPipeline.bind(null, req, res));
};

module.exports.findargoneMeta = function findargoneMeta (req, res, next, id, page) {

  apihits.apihits.create({metadata: req.openapi.openApiRoute, query: req.query, isWeb: req.headers.origin === 'https://argovis.colorado.edu', avhTelemetry: req.headers.hasOwnProperty('x-avh-telemetry') ? req.headers['x-avh-telemetry'] : null})
  
  Argone.findargoneMeta(res, id, page)
    .then(
      pipefittings => helpers.data_pipeline.bind(null, req, res, false)(pipefittings),
      helpers.lookupReject.bind(null, req, res)
    )
    .catch(helpers.catchPipeline.bind(null, req, res));
};
