/*
 * bucketdb
 *
 * Copyright(c) 2015 André König <andre.koenig@posteo.de>
 * MIT Licensed
 *
 */

/**
 * @author André König <andre.koenig@posteo.de>
 *
 */

'use strict';

var crypto = require('crypto');

var debug = require('debug')('bucketdb');

var mandatory = require('mandatory');
var jsonquery = require('jsonquery');
var VError = require('verror');

var storage = require('./storage');

module.exports = function instantiate (dbPath) {

    mandatory(dbPath).is('string', 'Please provide a proper database path');

    storage.setDatabasePath(dbPath);

    function database (bucketName) {
        var bucket = storage(bucketName);
        var bucketdb = new BucketDB(bucket);

        return {
            insert: bucketdb.insert.bind(bucketdb),
            update: bucketdb.update.bind(bucketdb),
            remove: bucketdb.remove.bind(bucketdb),
            query: bucketdb.query.bind(bucketdb)
        };
    }

    database.destroy = storage.destroy.bind(storage);

    return database;
};

/**
 * The storage abstraction.
 *
 * @param {string} db
 * The database "connection" object.
 *
 */
function BucketDB (bucket) {
    this.$bucket = bucket;
}

/**
 * @private
 *
 * Generates an id.
 *
 * @returns {string}
 *
 */
BucketDB.prototype.$generateId = function $generateId () {
    var hrtime = process.hrtime();
    var shasum = crypto.createHash('sha1');
    var id = '';

    shasum.update([Date.now(), hrtime[0], hrtime[1], Math.ceil(Math.random() * 10000)].join(''));
    id = shasum.digest('hex');

    debug('Created id: %s', id);

    return id;
};

/**
 * Inserts a data record.
 *
 * Please note that the data record will provide an attribute `id` afterwards.
 *
 * @param {object} record
 * The record which should be inserted.
 *
 * @param {function} callback
 * Will be executed when done as `callback(err, record)`.
 *
 */
BucketDB.prototype.insert = function insert (record, callback) {
    var self = this;

    mandatory(record).is('object', 'Please provide a proper "' + this.$type + '" record.');
    mandatory(callback).is('function', 'Please provide a proper callback function.');

    function onInsert (err) {
        if (err) {
            return callback(new VError(err, 'failed to insert %s: %s', self.$type, record));
        }

        callback(null, record);
    }

    if (!record.id) {
        record.id = this.$generateId();
    }

    this.$bucket.put(record.id, record, onInsert);
};

/**
 * Possibility to update a data record.
 * Please note that the record has to be persisted before you are able to
 * update it with the help of this method.
 *
 * @param {object} record
 * The data record which should be updated.
 *
 * @param {function} callback
 * Will be executed when done as `callback(err, record)`.
 *
 */
BucketDB.prototype.update = function update (record, callback) {
    var self = this;

    mandatory(record).is('object', 'Please provide a proper data record which should be updated.');
    mandatory(callback).is('function', 'Please provide a proper callback function.');

    if (!record.id) {
        return process.nextTick(function onTick () {
            var err = new VError('failed to update the record. The record does not have an id and therefore does not exist.');
            err.type = 'NotFoundError';

            callback(err);
        });
    }

    function onFind (err, found) {
        if (err) {
            return callback(new VError(err, 'failed to search for the record before updating it.'));
        }

        if (found.length === 0) {
            err = new VError('unable to update a non-existing record.');
            err.type = 'NotFoundError';

            return callback(err);
        }

        self.$bucket.put(record.id, record, onUpdate);
    }

    function onUpdate (err) {
        if (err) {
            return callback(new VError(err, 'failed to update the record.'));
        }

        callback(null, record);
    }

    this.query({id: record.id}, onFind);
};

/**
 * Remove a data record from the store.
 *
 * @param {string} id
 * The id of the data record which should be removed.
 *
 * @param {function} callback
 * Will be executed when done as `callback(err)`.
 *
 */
BucketDB.prototype.remove = function remove (id, callback) {
    mandatory(id).is('string', 'Please provide the id of the record which should be removed.');
    mandatory(callback).is('function', 'Please provide a proper callback function.');

    function onDelete (err) {
        if (err) {
            return callback(new VError(err, 'failed to delete the record with the id: %s', id));
        }

        callback(null);
    }

    this.$bucket.del(id, onDelete);
};

/**
 * Search for respective data records.
 *
 * @param {object} q
 * The query object, e.g.: {id: 'foo', name: 'bar'} Will search for all records
 * that have an id 'foo' and a name 'bar'.
 *
 * @param {function} callback
 * Will be executed when done as `callback(err, records)`.
 *
 */
BucketDB.prototype.query = function query (q, callback) {
    var results = [];
    var strom = null;

    mandatory(q).is('object', 'Please provide a proper query object.');
    mandatory(callback).is('function', 'Please provide a proper callback function.');

    function onData (record) {
        results.push(record);
    }

    function onError (err) {
        callback(new VError(err, 'failed to query the storage layer.'));
    }

    function onEnd () {
        callback(null, results);
    }

    strom = this.$bucket.createValueStream();

    // If an query has been defined -> pipe it through the 'jsonquery' module.
    if (Object.keys(q).length > 0) {
        strom = strom.pipe(jsonquery(q));
    }

    strom.on('data', onData)
        .once('error', onError)
        .once('end', onEnd);
};
