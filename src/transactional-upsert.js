var _ = require('lodash');

module.exports = (knex, options) => {
    options = options || {};
    options = _.defaults(options, {
        batchSize: 100,
    });

    return (tableName, rows, fieldToTypeMap, primaryKey) => {
        var makeBatches = (rows) => {
            var batches = [];

            for (var fromIndex = 0; fromIndex < rows.length; fromIndex += options.batchSize) {
                batches.push(rows.slice(fromIndex, Math.min(rows.length, fromIndex + options.batchSize)));
            }

            return batches;
        };

        var doBatchOfUpdates = (txn, rows) => {
            /*
                Do multiple updates like this:
                update test as t set
                    column_a = c.column_a,
                    column_c = c.column_c
                    from (values
                    ('123', 1, '---'),
                    ('345', 2, '+++')
                ) as c(column_b, column_a, column_c)
                where c.column_b = t.column_b;
            */

            var updateQuery = (
                `UPDATE ${tableName} AS t SET `
            );

            var fields = Object.keys(rows[0]);

            updateQuery += fields.map(field => ` ${field} = CAST(u.${field} AS ${fieldToTypeMap[field]})`).join(', ');

            var bindings = [];

            var valuesString = rows.map(row => {
                bindings.push.apply(bindings, fields.map(field => row[field]));

                return `(${Object.keys(row).map(rowValue => `?`).join(', ')})`;
            }).join(', ');

            updateQuery += ` FROM (values ${valuesString} ) AS u (${Object.keys(rows[0]).join(', ')})`;

            var primaryKeyArray = Array.isArray(primaryKey) ? primaryKey : [primaryKey];
            var where = primaryKeyArray.map(key => ` t.${key} = u.${key}::${fieldToTypeMap[key]} `).join(' AND ');

            updateQuery += ` WHERE ${where}`;

            return knex.raw(updateQuery, bindings).transacting(txn);
        };

        var doUpdates = (txn, rows) => {
            if (rows.length === 0) {
                return new Promise(resolve => resolve());
            }

            return Promise.all(
                makeBatches(rows).map(batch => {
                    return doBatchOfUpdates(txn, batch);
                })
            );
        };

        var doBatchOfInserts = (txn, rows) => {
            return knex(tableName).insert(rows).transacting(txn);
        };

        var doInserts = (txn, rows) => {
            if (rows.length === 0) {
                return new Promise(resolve => resolve());
            }

            return Promise.all(
                makeBatches(rows).map(batch => {
                    return doBatchOfInserts(txn, batch);
                })
            );
        };

        var query = knex(tableName);

        var updates = [];
        var inserts = [];

        return query.then((collection) => {
            return knex.transaction((txn) => {
                rows.forEach((row) => {
                    var predicate = {};
                    if (Array.isArray(primaryKey)) {
                        primaryKey.forEach(keyCol => {
                            predicate[keyCol] = row[keyCol];
                        });
                    } else {
                        predicate[primaryKey] = row[primaryKey];
                    }

                    var exists = _.find(collection, predicate);
                    if (exists) {
                        // ups
                        updates.push(row);
                    } else {
                        // serts
                        inserts.push(row);
                    }
                });

                return Promise.all([
                    doUpdates(txn, updates),
                    doInserts(txn, inserts)
                ]);
            });
        });
    };
};
