const mongoose = require('mongoose');

const talk_schema = new mongoose.Schema({
    Evento: String,
    title: String,
    url: String,
    details: String,
    main_author: String
}, { collection: 'tedz_data' });

module.exports = mongoose.model('talk', talk_schema);