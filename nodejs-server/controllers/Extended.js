'use strict';

var utils = require('../utils/writer.js');
var Extended = require('../service/ExtendedService');

module.exports.extendedVocab = function extendedVocab (req, res, next, extendedName, parameter) {
  Extended.extendedVocab(extendedName, parameter)
    .then(function (response) {
      utils.writeJson(res, response);
    })
    .catch(function (response) {
      utils.writeJson(res, response);
    });
};

module.exports.findExtended = function findExtended (req, res, next, extendedName, id, startDate, endDate, polygon, box, center, radius, compression, data, batchmeta) {
  Extended.findExtended(extendedName, id, startDate, endDate, polygon, box, center, radius, compression, data, batchmeta)
    .then(function (response) {
      utils.writeJson(res, response);
    })
    .catch(function (response) {
      utils.writeJson(res, response);
    });
};

module.exports.findextendedMeta = function findextendedMeta (req, res, next, id) {
  Extended.findextendedMeta(id)
    .then(function (response) {
      utils.writeJson(res, response);
    })
    .catch(function (response) {
      utils.writeJson(res, response);
    });
};
