'use strict';
const apihits = require('../models/apihits');
var helpers = require('../helpers/helpers')
var SummaryExperimental = require('../service/SummaryExperimentalService');

module.exports.fetchSummary = function fetchSummary (req, res, next, id, key) {

  apihits.apihits.create({metadata: req.openapi.openApiRoute, query: req.query, isWeb: req.headers.origin === 'https://argovis.colorado.edu', avhTelemetry: req.headers.hasOwnProperty('x-avh-telemetry') ? req.headers['x-avh-telemetry'] : null})
  
  SummaryExperimental.fetchSummary(id, key)
    .then(
      helpers.simpleWrite.bind(null, req, res),
      helpers.lookupReject.bind(null, req, res)
    )
    .catch(helpers.catchPipeline.bind(null, req, res));
};