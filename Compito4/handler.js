const connect_to_db = require('./db');

// GET BY TALK HANDLER

const talk = require('./Talk');

module.exports.handler = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));
    let body = {}
    if (event.body) {
        body = JSON.parse(event.body)
    }
    // set default
    if(!body.id) {
        callback(null, {
                    statusCode: 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the talks. Tag is null.'
        })
    } 
        if(body.id.length!=32) {
        callback(null, {
                    statusCode: 400,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Bad Request'
        })
    }
    

    connect_to_db().then(() => 
        {
        console.log('=> get_all url'); //inizio cercando tutti i video watch_next dal db
        talk.find({_id: body.id},{watch_next_url:1,_id:1})
        .then(talks => 
            {
            var temp=JSON.stringify(talks)  //sposto l'array in JSON con l'obiettivo di ottenre un obj javascript
            var u=JSON.parse(temp) 
            console.log('=> get_all next');
            talk.find({url: {$in: u[0].watch_next_url}})  //trovo tutti i match con l'array
            .then(talks => 
                {
                if(talks.length!=0){ callback(null, {statusCode: 200,body: JSON.stringify(talks)}) //può essere che un video non ha nessun watchnext nel db
                 }else{callback(null, {statusCode: 404,headers: { 'Content-Type': 'text/plain' },body: 'data talk not found'} )}
                })
             })
            .catch(err => callback(null, {statusCode: err.statusCode || 500, headers: { 'Content-Type': 'text/plain' }, body: 'Could not fetch the next'})
                    );
        });
};
