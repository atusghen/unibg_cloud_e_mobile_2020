const connect_to_db = require('./db'); //carica db.js

// GET BY TALK HANDLER

const talk = require('./Talk'); //carica Talk.js

module.exports.get_watch_idx= (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));
    let body = {}
    if (event.body) {
        body = JSON.parse(event.body) //contenuto evento nel campo body trasformato in JSON
    }
    // set default
    if(!body.id) { //errore se non è presente l'id da ricercare
        callback(null, {
                    statusCode: 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the talks. Id is null.'
        })
    }
    


    connect_to_db().then(() => {
        console.log('=> get_all talks');
        talk.find({_id: body.id}, {watch_next_url: 1, _id:1}) //talk definito sopra
            .then(talks => {
                    callback(null, {
                        statusCode: 200,
                        body: JSON.stringify(talks)
                    })
                }
            )
            .catch(err =>
                callback(null, {
                    statusCode: err.statusCode || 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Cannot fetch'
                })
            );
    });
};