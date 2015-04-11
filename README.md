# BucketDB

Personal convenience wrapper around LevelDB with a minimal CRUD-based API.

## Installation

```sh
npm install --save bucketdb
```

## Usage

```js

var bucketdb = require('bucketdb')('/path/to/your/database');

var bucket = bucketdb('bucket-name');

```

## API

### `bucketdb(bucketName)`

This is a factory method which will create a bucket for the given entity name. Please note that the storage layer will create a new entity namespace if not available.

### `bucket.insert(record, callback)`

Will insert a new record. Please note that this method will create a new id for this record.

```js
var bucket = bucketdb('account');
var user = {name: 'André König'};

function onInsert (err, user) {
    if (err) {
        return console.error('Meh: ' + err.toString());
    }

    console.log(user.id); // => 6016dfffa938f0a1dcb66d4630a975925fc4309b
}

bucket.insert(user, onInsert);
```

**Important note:** This method does not distinguish between several different objects. Means, if you pass an already persisted object this method will store a second record with a different id in the database.

### `bucket.update(record, callback)`

Will update an existing record:

```js
var bucket = bucketdb('account');
var user = {
    id: '6016dfffa938f0a1dcb66d4630a975925fc4309b',
    name: 'André König'
};

function onUpdate (err, user) {
    if (err) {
        return console.error('Meh: ' + err.toString());
    }

    console.log('Updated!!');
}

bucket.update(user, onUpdate);

```

### `bucket.remove(id, callback)`

Will delete an existing record.

```js

var bucket = bucketdb('account');

function onRemove (err) {
    if (err) {
        return console.error('Meh: ' + err.toString());
    }

    console.log('Removed!!');
}

bucket.remove('6016dfffa938f0a1dcb66d4630a975925fc4309b', onRemove);

```

### `bucket.query(q, callback)`

Possibility to send queries to the persistence store. This method supports the notation of the [MongoDB query language](http://docs.mongodb.org/manual/tutorial/query-documents/).

```js

var bucket = bucketdb('account');

var query = {
    id: {
        $in: ['6016dfffa938f0a1dcb66d4630a975925fc4309b', 'f5f0431efda54374889553f27a6710cfc48a69e7'];
};

function onQuery (err, results) {
    if (err) {
        return console.error('Meh: ' + err.toString());
    }

    console.log(results); // An array with the results (even when there is only one record; empty array when no results are available).
}

bucket.query({id: {$in: ids}, onQuery);
```

## Tests

```sh
npm test
```

## License

The MIT License (MIT)

Copyright (c) 2015 André König, Hamburg, Germany <akoenig@posteo.de>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
