const mongoose = require('mongoose');
const Schema = mongoose.Schema;

const apihits = Schema({
	metadata: {type: String, required: true},
	query: {type:Schema.Types.Mixed, required: false},
	product: {type: String, required: false},
	isWeb: {type: String, required: false},
	avhTelemetry: {type: String, required: false},
}, { timestamps: {createdAt: 'timestamp'} })

module.exports = {}
module.exports.apihits = mongoose.model('apihits', apihits, 'apihits');