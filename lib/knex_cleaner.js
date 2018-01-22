'use strict';

var BPromise = require('bluebird');
var _ = require('lodash');

var knexTables = require('../lib/knex_tables');

var DefaultOptions = {
  mode: 'truncate',    // Can be ['truncate', 'delete']
  ignoreTables: []     // List of tables to not delete
};

// https://tc39.github.io/ecma262/#sec-array.prototype.includes
if (!Array.prototype.includes) {
  Object.defineProperty(Array.prototype, 'includes', {
    value: function(searchElement, fromIndex) {

      if (this == null) {
        throw new TypeError('"this" is null or not defined');
      }

      // 1. Let O be ? ToObject(this value).
      var o = Object(this);

      // 2. Let len be ? ToLength(? Get(O, "length")).
      var len = o.length >>> 0;

      // 3. If len is 0, return false.
      if (len === 0) {
        return false;
      }

      // 4. Let n be ? ToInteger(fromIndex).
      //    (If fromIndex is undefined, this step produces the value 0.)
      var n = fromIndex | 0;

      // 5. If n ≥ 0, then
      //  a. Let k be n.
      // 6. Else n < 0,
      //  a. Let k be len + n.
      //  b. If k < 0, let k be 0.
      var k = Math.max(n >= 0 ? n : len - Math.abs(n), 0);

      function sameValueZero(x, y) {
        return x === y || (typeof x === 'number' && typeof y === 'number' && isNaN(x) && isNaN(y));
      }

      // 7. Repeat, while k < len
      while (k < len) {
        // a. Let elementK be the result of ? Get(O, ! ToString(k)).
        // b. If SameValueZero(searchElement, elementK) is true, return true.
        if (sameValueZero(o[k], searchElement)) {
          return true;
        }
        // c. Increase k by 1. 
        k++;
      }

      // 8. Return false
      return false;
    }
  });
}

function clean(knex, passedInOptions) {
  var options = _.defaults({}, passedInOptions, DefaultOptions);

  return knexTables.getTableNames(knex, options)
  .then(function(tables) {
    var finalTables = null;
    if (options.order) {
      finalTables = options.order;
      for (var table in tables) {
        if (!finalTables.includes(table)) {
          finalTables.push(table);
        }
      }
    }
    else {
      finalTables = tables;
    }
    if (options.mode === 'delete') {
      return cleanTablesWithDeletion(knex, finalTables, options);
    } else {
      return cleanTablesWithTruncate(knex, finalTables, options);
    }
  });
}

function cleanTablesWithDeletion(knex, tableNames, options) {
  return BPromise.map(tableNames, function(tableName) {
    return knex.select().from(tableName).del();
  });
}

function cleanTablesWithTruncate(knex, tableNames, options) {
  var client = knex.client.dialect;

  switch(client) {
    case 'mysql':
      return knex.transaction(function(trx) {
        knex.raw('SET FOREIGN_KEY_CHECKS=0').transacting(trx)
        .then(function() {
          return BPromise.map(tableNames, function(tableName) {
            return knex(tableName).truncate().transacting(trx);
          });
        })
        .then(function() {
          return knex.raw('SET FOREIGN_KEY_CHECKS=1').transacting(trx);
        })
        .then(trx.commit);
      });
    case 'postgresql':
      if (_.has(tableNames, '[0]')) {
        var quotedTableNames = tableNames.map(function(tableName) {
          return '\"' + tableName + '\"';
        });
        return knex.raw('TRUNCATE ' + quotedTableNames.join() + ' CASCADE');
      }
      return;
    case 'sqlite3':
      return BPromise.map(tableNames, function(tableName) {
        return knex(tableName).truncate();
      });
    default:
      throw new Error('Could not get the sql to select table names from client: ' + client);
  }
}

module.exports = {
  clean: function(knex, options) {
    return clean(knex, options);
  }
};
