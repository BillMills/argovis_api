'use strict';

var utils = require('../utils/writer.js');
var Timeseries = require('../service/TimeseriesService');

module.exports.findtimeseries = function findtimeseries (req, res, next, timeseriesName, id, startDate, endDate, polygon, box, center, radius, compression, mostrecent, data, batchmeta, page) {
  Timeseries.findtimeseries(timeseriesName, id, startDate, endDate, polygon, box, center, radius, compression, mostrecent, data, batchmeta, page)
    .then(function (response) {
      utils.writeJson(res, response);
    })
    .catch(function (response) {
      utils.writeJson(res, response);
    });
};

module.exports.findtimeseriesMeta = function findtimeseriesMeta (req, res, next, id, page) {
  Timeseries.findtimeseriesMeta(id, page)
    .then(function (response) {
      utils.writeJson(res, response);
    })
    .catch(function (response) {
      utils.writeJson(res, response);
    });
};

module.exports.timeseriesVocab = function timeseriesVocab (req, res, next, timeseriesName, parameter) {
  Timeseries.timeseriesVocab(timeseriesName, parameter)
    .then(function (response) {
      utils.writeJson(res, response);
    })
    .catch(function (response) {
      utils.writeJson(res, response);
    });
};
