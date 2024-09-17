'use strict';
const apihits = require('../models/apihits');
var ArgoTrajectoriesExperimental = require('../service/ArgoTrajectoriesExperimentalService');
var helpers = require('../helpers/helpers')

module.exports.argotrajectoryVocab = function argotrajectoryVocab (req, res, next, parameter) {

  apihits.apihits.create({metadata: req.openapi.openApiRoute, query: req.query, isWeb: req.headers.origin === 'https://argovis.colorado.edu', avhTelemetry: req.headers.hasOwnProperty('x-avh-telemetry') ? req.headers['x-avh-telemetry'] : null})

  ArgoTrajectoriesExperimental.argotrajectoryVocab(parameter)
    .then(
      helpers.simpleWrite.bind(null, req, res),
      helpers.lookupReject.bind(null, req, res)
    )
    .catch(helpers.catchPipeline.bind(null, req, res));
};

module.exports.findArgoTrajectory = function findArgoTrajectory (req, res, next, id, startDate, endDate, polygon, box, center, radius, metadata, platform, compression, mostrecent, data, batchmeta) {

  apihits.apihits.create({metadata: req.openapi.openApiRoute, query: req.query, isWeb: req.headers.origin === 'https://argovis.colorado.edu', avhTelemetry: req.headers.hasOwnProperty('x-avh-telemetry') ? req.headers['x-avh-telemetry'] : null})

  ArgoTrajectoriesExperimental.findArgoTrajectory(res, id, startDate, endDate, polygon, box, center, radius, metadata, platform, compression, mostrecent, data, batchmeta)
    .then(
      pipefittings => helpers.data_pipeline.bind(null, req, res, batchmeta)(pipefittings),
      helpers.lookupReject.bind(null, req, res)
    )
    .catch(helpers.catchPipeline.bind(null, req, res));
};

module.exports.findArgotrajectorymeta = function findArgotrajectorymeta (req, res, next, id, platform) {

  apihits.apihits.create({metadata: req.openapi.openApiRoute, query: req.query, isWeb: req.headers.origin === 'https://argovis.colorado.edu', avhTelemetry: req.headers.hasOwnProperty('x-avh-telemetry') ? req.headers['x-avh-telemetry'] : null})
  
  ArgoTrajectoriesExperimental.findArgotrajectorymeta(res, id, platform)
    .then(
      pipefittings => helpers.data_pipeline.bind(null, req, res, false)(pipefittings),
      helpers.lookupReject.bind(null, req, res)
    )
    .catch(helpers.catchPipeline.bind(null, req, res));
};
