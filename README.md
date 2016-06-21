### Usage

Make it into an electrolyte component like so:

```js
var getUpserters = require('knex-postgres-transactional-upsert');

module.exports = (knex) => getUpserters(knex, {batchSize: 500});

module.exports['@singleton'] = true;
module.exports['@require'] = ['knex'];
```

Then use it like so:
```js
// Upsert a single row without a transaction
upsert.upsert('tablename', id, data);
// Upsert many rows transactionally
upsert.transactionalUpsert('tablename', rows, fieldToTypeMap, primaryKey);
```
