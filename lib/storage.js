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

var path = require('path');
var debug = require('debug')('bucketdb:database');
var levelup = require('levelup');
var leveldown = require('leveldown');
var mandatory = require('mandatory');
var sublevel = require('level-sublevel');
var VError = require('verror');

var DB_PATH = null;
var INSTANCE = null;

function database () {

    DB_PATH = DB_PATH || path.join(process.cwd(), 'bucketdb');

    if (!INSTANCE) {
        debug('Create new LevelDB instance.');

        INSTANCE = levelup(DB_PATH, {
            valueEncoding: 'json'
        });
    }

    return INSTANCE;
}

var wrapper = module.exports = function wrapper (dbPath, type) {

    mandatory(type).is('string', 'Please define a storage entity type.');

    DB_PATH = dbPath;

    //
    // Create a sublevel of the LevelDB instance and pass it to
    // the storage service. With this approach it is possible to abstract the
    // whole sublevel aspects away.
    //
    // TODO: The `sublevel` activation can be moved to `database.getInstance` when
    // https://github.com/dominictarr/level-sublevel/issues/78 has been closed.
    //
    return sublevel(database()).sublevel(type);
};

/**
 * Function for destroying the whole storage layer.
 *
 * @param {function} callback
 * Will be executed when the storage layer and the respective database has
 * been destroyed. Executed as `callback(err)`.
 *
 */
wrapper.destroy = function destroy (callback) {
    mandatory(callback).is('function', 'Please provide a proper callback function.');

    function onDestroy (err) {
        if (err) {
            return callback(new VError(err, 'failed to destroy the storage layer.'));
        }

        callback(null);
    }

    function onClose () {
        leveldown.destroy(DB_PATH, onDestroy);
    }

    return database().once('closed', onClose).close();
};
