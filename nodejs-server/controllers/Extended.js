'use strict';

const apihits = require('../models/apihits');
var utils = require('../utils/writer.js');
var Extended = require('../service/ExtendedService');
var helpers = require('../helpers/helpers')

module.exports.extendedVocab = function extendedVocab (req, res, next, parameter, extendedName) {

  apihits.apihits.create({metadata: req.openapi.openApiRoute, query: req.query, product: extendedName, isWeb: req.headers.origin === 'https://argovis.colorado.edu'})
  
  Extended.extendedVocab(extendedName, parameter)
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

module.exports.findExtended = function findExtended (req, res, next, id, startDate, endDate, polygon, box, winding, center, radius, compression, mostrecent, data, batchmeta, extendedName) {
  
  apihits.apihits.create({metadata: req.openapi.openApiRoute, query: req.query, product: extendedName, isWeb: req.headers.origin === 'https://argovis.colorado.edu'})
  
  Extended.findExtended(res,extendedName,id,startDate,endDate,polygon,box,winding,center,radius,compression,mostrecent,data,batchmeta)
    .then(pipefittings => helpers.data_pipeline.bind(null, res, batchmeta)(pipefittings),
    function (response) {
      utils.writeJson(res, response, response.code);
    })
    .catch(function (response) {
      utils.writeJson(res, response);
    });
};

module.exports.findextendedMeta = function findextendedMeta (req, res, next, id) {

  apihits.apihits.create({metadata: req.openapi.openApiRoute, query: req.query, isWeb: req.headers.origin === 'https://argovis.colorado.edu'})

  Extended.findextendedMeta(res, id)
    .then(pipefittings => helpers.data_pipeline.bind(null, res, false)(pipefittings),
    function (response) {
      utils.writeJson(res, response, response.code);
    })
    .catch(function (response) {
      utils.writeJson(res, response);
    });
};

