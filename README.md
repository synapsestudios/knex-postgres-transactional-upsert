### Usage

Make it into an electrolyte component like so:

```js
var transactionalUpsert = require('knex-postgres-transactional-upsert');

module.exports = (knex) => transactionalUpsert(knex, {batchSize: 500});

module.exports['@singleton'] = true;
module.exports['@require'] = ['knex'];
```
