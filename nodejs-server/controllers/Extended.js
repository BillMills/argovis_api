'use strict';

const apihits = require('../models/apihits');
var utils = require('../utils/writer.js');
var Extended = require('../service/ExtendedService');
var helpers = require('../helpers/helpers')

module.exports.extendedVocab = function extendedVocab (req, res, next, parameter, extendedName) {

  apihits.apihits.create({metadata: req.openapi.openApiRoute, query: req.query, product: extendedName, isWeb: req.headers.origin === 'https://argovis.colorado.edu', avhTelemetry: req.headers.hasOwnProperty('x-avh-telemetry') ? req.headers['x-avh-telemetry'] : null})
  
  Extended.extendedVocab(extendedName, parameter)
    .then(
      helpers.simpleWrite.bind(null, req, res),
      helpers.lookupReject.bind(null, req, res)
    )
    .catch(helpers.catchPipeline.bind(null, req, res));
};

module.exports.findExtended = function findExtended (req, res, next, id, startDate, endDate, polygon, box, center, radius, compression, mostrecent, data, batchmeta, extendedName) {
  
  apihits.apihits.create({metadata: req.openapi.openApiRoute, query: req.query, product: extendedName, isWeb: req.headers.origin === 'https://argovis.colorado.edu', avhTelemetry: req.headers.hasOwnProperty('x-avh-telemetry') ? req.headers['x-avh-telemetry'] : null})
  
  Extended.findExtended(res,extendedName,id,startDate,endDate,polygon,box,center,radius,compression,mostrecent,data,batchmeta)
    .then(
      pipefittings => helpers.data_pipeline.bind(null, req, res, batchmeta)(pipefittings),
      helpers.lookupReject.bind(null, req, res)
    )
    .catch(helpers.catchPipeline.bind(null, req, res));
};

module.exports.findextendedMeta = function findextendedMeta (req, res, next, id) {

  apihits.apihits.create({metadata: req.openapi.openApiRoute, query: req.query, isWeb: req.headers.origin === 'https://argovis.colorado.edu', avhTelemetry: req.headers.hasOwnProperty('x-avh-telemetry') ? req.headers['x-avh-telemetry'] : null})

  Extended.findextendedMeta(res, id)
    .then(
      pipefittings => helpers.data_pipeline.bind(null, req, res, false)(pipefittings),
      helpers.lookupReject.bind(null, req, res)
    )
    .catch(helpers.catchPipeline.bind(null, req, res));
};

