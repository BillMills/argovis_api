'use strict';

var utils = require('../utils/writer.js');
var SummaryExperimental = require('../service/SummaryExperimentalService');

module.exports.fetchSummary = function fetchSummary (req, res, next, id, key) {
  SummaryExperimental.fetchSummary(id, key)
    .then(function (response) {
      utils.writeJson(res, response);
    })
    .catch(function (response) {
      utils.writeJson(res, response);
    });
};
