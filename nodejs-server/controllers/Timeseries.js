'use strict';

const apihits = require('../models/apihits');
var utils = require('../utils/writer.js');
var Timeseries = require('../service/TimeseriesService');
var helpers = require('../helpers/helpers')

module.exports.findtimeseries = function findtimeseries (req, res, next, id, startDate, endDate, polygon, box, center, radius, presRange, compression, mostrecent, data, batchmeta, timeseriesName) {

  apihits.apihits.create({metadata: req.openapi.openApiRoute, query: req.query, product: timeseriesName, isWeb: req.headers.origin === 'https://argovis.colorado.edu', avhTelemetry: req.headers.hasOwnProperty('x-avh-telemetry') ? req.headers['x-avh-telemetry'] : null})
  
  Timeseries.findtimeseries(res, timeseriesName, id, startDate, endDate, polygon, box, center, radius, presRange, compression, mostrecent, data, batchmeta)
    .then(pipefittings => helpers.data_pipeline.bind(null, res, batchmeta)(pipefittings),
    function (response) {
      utils.writeJson(res, response, response.code);
    })
    .catch(function (response) {
      utils.writeJson(res, response);
    });
};

module.exports.findtimeseriesMeta = function findtimeseriesMeta (req, res, next, id) {
  
  apihits.apihits.create({metadata: req.openapi.openApiRoute, query: req.query, isWeb: req.headers.origin === 'https://argovis.colorado.edu', avhTelemetry: req.headers.hasOwnProperty('x-avh-telemetry') ? req.headers['x-avh-telemetry'] : null})

  Timeseries.findtimeseriesMeta(res, id)
    .then(pipefittings => helpers.data_pipeline.bind(null, res, false)(pipefittings),
    function (response) {
      utils.writeJson(res, response, response.code);
    })
    .catch(function (response) {
      utils.writeJson(res, response);
    });
};

module.exports.timeseriesVocab = function timeseriesVocab (req, res, next, parameter, timeseriesName) {
  
  apihits.apihits.create({metadata: req.openapi.openApiRoute, query: req.query, product: timeseriesName, isWeb: req.headers.origin === 'https://argovis.colorado.edu', avhTelemetry: req.headers.hasOwnProperty('x-avh-telemetry') ? req.headers['x-avh-telemetry'] : null})

  Timeseries.timeseriesVocab(timeseriesName, parameter)
    .then(function (response) {
      utils.writeJson(res, response);
    },
    function (response) {
      utils.writeJson(res, response, response.code);
    })
    .catch(function (response) {
      utils.writeJson(res, response);
    });
};
