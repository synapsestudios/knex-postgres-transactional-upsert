var transactionalUpsert = require('./src/transactional-upsert');
var upsert = require('./src/upsert');

module.exports = (knex, options) => {
    return {
        transactionalUpsert: transactionalUpsert(knex, options),
        upsert: upsert(knex),
    };
};
