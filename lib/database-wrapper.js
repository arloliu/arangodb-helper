const arangojs = require('arangojs');
const CollectionHelper = require('./collection-helper');
const ArangoDbHelper = require('./arangodb-helper');

class DatabaseWrapper extends arangojs.Database {
    constructor(config) {
        super(config);
        this._revokeTokenTimer = null;
        this._acquireHostListTimer = null;
        this.aql = arangojs.aql;
    }

    collection(collectionName) {
        const collection = super.collection(collectionName);
        CollectionHelper.injectCollection(collection);
        return collection;
    }

    shutdown() {
        if (this._acquireHostListTimer) {
            clearInterval(this._acquireHostListTimer);
            this._acquireHostListTimer = null;
        }
        if (this._revokeTokenTimer) {
            clearInterval(this._revokeTokenTimer);
            this._revokeTokenTimer = null;
        }
        this.close();
    }

    getDbHelper(collectionName) {
        return new ArangoDbHelper(this, collectionName);
    }

    async queryOne(query, bindVars, opts) {
        const cursor = await this.query(query, bindVars, opts);
        if (!cursor.hasNext()) {
            return null;
        }

        return await cursor.next();
    }

    async queryAll(query, bindVars, opts) {
        const cursor = await this.query(query, bindVars, opts);
        if (!cursor.hasNext()) {
            return [];
        }

        return await cursor.all();
    }
}

module.exports = DatabaseWrapper;
