'use strict';

var utils = require('../utils/writer.js');
var Cchdo = require('../service/CchdoService');

module.exports.cchdoVocab = function cchdoVocab (req, res, next, parameter) {
  Cchdo.cchdoVocab(parameter)
    .then(function (response) {
      utils.writeJson(res, response);
    })
    .catch(function (response) {
      utils.writeJson(res, response);
    });
};

module.exports.findCCHDO = function findCCHDO (req, res, next, id, startDate, endDate, polygon, box, center, radius, metadata, woceline, cchdo_cruise, source, compression, mostrecent, data, presRange, verticalRange, batchmeta, page) {
  Cchdo.findCCHDO(id, startDate, endDate, polygon, box, center, radius, metadata, woceline, cchdo_cruise, source, compression, mostrecent, data, presRange, verticalRange, batchmeta, page)
    .then(function (response) {
      utils.writeJson(res, response);
    })
    .catch(function (response) {
      utils.writeJson(res, response);
    });
};

module.exports.findCCHDOmeta = function findCCHDOmeta (req, res, next, id, woceline, cchdo_cruise, page) {
  Cchdo.findCCHDOmeta(id, woceline, cchdo_cruise, page)
    .then(function (response) {
      utils.writeJson(res, response);
    })
    .catch(function (response) {
      utils.writeJson(res, response);
    });
};
