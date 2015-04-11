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

var fs = require('fs');
var path = require('path');

var storage = require('../')(require('path').join(process.cwd(), 'database'));

var expect = require('expect.js');

describe('The storage layer should provide a factory that is capable', function suite () {

    var db = path.join(process.cwd(), 'bucketdb');

    before(storage.destroy);

    it('to create a LevelDB instance', function test (done) {
        storage('foo');

        function onStat (err) {
            expect(err).to.be(null);

            done();
        }

        fs.stat(db, onStat);
    });
});
