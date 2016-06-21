module.exports = (knex) => {
    var upsertRow = (table, id, data) => {
        var keys = Object.keys(data);
        if (typeof id === 'string') {
            id = [id];
        }
        var keysWithoutId = keys.filter(key => {return id.indexOf(key) === -1;});
        var vals = keys.map(key => data[key]);

        var sets = keysWithoutId.map(() => '??=?').join(',');

        var bindings = [];
        keysWithoutId.forEach(key => {
            bindings.push(key, data[key]);
        });
        var wheres = id.map(() => '??=?').join(' AND ');
        id.forEach(key => {
            bindings.push(key, data[key]);
        });

        var insertCols = vals.map(() => '??').join(',');
        bindings.push.apply(bindings, keys);
        var insertVals = vals.map(() => '?').join(',');
        bindings.push.apply(bindings, vals);

        return knex.raw(
            `WITH upsert AS (UPDATE ${table} SET ${sets} WHERE ${wheres} RETURNING *) ` +
            `INSERT INTO ${table} (${insertCols}) SELECT ${insertVals} WHERE NOT EXISTS (SELECT * FROM upsert)`,
            bindings
        );
    };

    return (table, id, data) => {
        if (Array.isArray(data)) {
            return Promise.all(data.map(row => upsertRow(table, id, row)));
        } else {
            return upsertRow(table, id, data);
        }
    };
};
