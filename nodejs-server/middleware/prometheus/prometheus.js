const express = require('express');
const router = express.Router();
const client = require('prom-client');

// router.get('/metrics', (req, res) => {
//   res.set('Content-Type', client.register.contentType);
//   res.end(client.register.metrics());
// });

router.get('/metrics', (req,res) => {
    res.set('Content-Type', client.register.contentType)
    client.register.metrics().then(data => res.send(data));
})

module.exports = router;