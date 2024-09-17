'use strict';

var Ping = require('../service/PingService');
var helpers = require('../helpers/helpers')

module.exports.ping = function ping (req, res, next) {
  Ping.ping()
    .then(
      helpers.simpleWrite.bind(null, req, res),
      helpers.lookupReject.bind(null, req, res)
    )
    .catch(helpers.catchPipeline.bind(null, req, res));
};