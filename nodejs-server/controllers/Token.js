'use strict';
const apihits = require('../models/apihits');
var utils = require('../utils/writer.js');
var Token = require('../service/TokenService');
var helpers = require('../helpers/helpers')

module.exports.validateToken = function validateToken (req, res, next, token) {

  apihits.apihits.create({metadata: req.openapi.openApiRoute, query: req.query, isWeb: req.headers.origin === 'https://argovis.colorado.edu', avhTelemetry: req.headers.hasOwnProperty('x-avh-telemetry') ? req.headers['x-avh-telemetry'] : null})
  
  Token.validateToken(res, token)
    .then(
      pipefittings => helpers.data_pipeline.bind(null, req, res, false)(pipefittings),
      helpers.lookupReject.bind(null, req, res)
    )
    .catch(helpers.catchPipeline.bind(null, req, res));
};
