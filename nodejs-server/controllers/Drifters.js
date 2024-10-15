'use strict';

var utils = require('../utils/writer.js');
var Drifters = require('../service/DriftersService');

module.exports.drifterMetaSearch = function drifterMetaSearch (req, res, next, id, platform, wmo, page) {
  Drifters.drifterMetaSearch(id, platform, wmo, page)
    .then(function (response) {
      utils.writeJson(res, response);
    })
    .catch(function (response) {
      utils.writeJson(res, response);
    });
};

module.exports.drifterSearch = function drifterSearch (req, res, next, id, startDate, endDate, polygon, box, center, radius, metadata, wmo, platform, compression, mostrecent, data, batchmeta, page) {
  Drifters.drifterSearch(id, startDate, endDate, polygon, box, center, radius, metadata, wmo, platform, compression, mostrecent, data, batchmeta, page)
    .then(function (response) {
      utils.writeJson(res, response);
    })
    .catch(function (response) {
      utils.writeJson(res, response);
    });
};

module.exports.drifterVocab = function drifterVocab (req, res, next, parameter) {
  Drifters.drifterVocab(parameter)
    .then(function (response) {
      utils.writeJson(res, response);
    })
    .catch(function (response) {
      utils.writeJson(res, response);
    });
};
