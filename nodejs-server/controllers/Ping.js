'use strict';

var utils = require('../utils/writer.js');
var Ping = require('../service/PingService');

module.exports.ping = function ping (req, res, next) {
  Ping.ping()
    .then(
      helpers.simpleWrite.bind(null, req, res),
      helpers.lookupReject.bind(null, req, res)
    )
    .catch(helpers.catchPipeline.bind(null, req, res));
};