'use strict';
const apihits = require('../models/apihits');
var utils = require('../utils/writer.js');
var Grid = require('../service/GridService');
var helpers = require('../helpers/helpers')

module.exports.findgrid = function findgrid (req, res, next, id, startDate, endDate, polygon, box, winding, center, radius, compression, mostrecent, data, presRange, batchmeta, gridName) {
 
  apihits.apihits.create({metadata: req.openapi.openApiRoute, query: req.query, product: gridName, isWeb: req.headers.origin === 'https://argovis.colorado.edu'})

  Grid.findgrid(res,gridName, id, startDate, endDate, polygon, box, winding, center, radius, compression, mostrecent, data, presRange, batchmeta)
    .then(pipefittings => helpers.data_pipeline.bind(null, res, batchmeta)(pipefittings),
    function (response) {
      utils.writeJson(res, response, response.code);
    })
    .catch(function (response) {
      utils.writeJson(res, response);
    });
};

module.exports.findgridMeta = function findgridMeta (req, res, next, id) {

  apihits.apihits.create({metadata: req.openapi.openApiRoute, query: req.query, isWeb: req.headers.origin === 'https://argovis.colorado.edu'})

  Grid.findgridMeta(res,id)
    .then(pipefittings => helpers.data_pipeline.bind(null, res, false)(pipefittings),
    function (response) {
      utils.writeJson(res, response, response.code);
    })
    .catch(function (response) {
      utils.writeJson(res, response);
    });
};

module.exports.gridVocab = function gridVocab (req, res, next, parameter, gridName) {
  
  apihits.apihits.create({metadata: req.openapi.openApiRoute, query: req.query, product: gridName, isWeb: req.headers.origin === 'https://argovis.colorado.edu'})
  
  Grid.gridVocab(gridName, parameter)
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
