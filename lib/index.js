const _ = require('lodash');
const arangojs = require('arangojs');
const DatabaseWrapper = require('./database-wrapper');
const Util = require('./util');

let arangoDbClient = null;

async function checkConnection(db) {
    try {
        await db.version();
        return true;
    } catch (err) {
        if (_.isNumber(err.code) && err.code < 500) {
            return true;
        }
        return false;
    }
}

async function waitDatabaseReady(db) {
    if (!await checkConnection(db)) {
        return new Promise((resolve, reject) => {
            const timer = setInterval(() => {
                checkConnection(db).then((result) => {
                    if (result) {
                        clearInterval(timer);
                        resolve(true);
                    }
                });
            }, 500);
        });
    }
    return true;
}

async function login(db, username, password) {
    try {
        const token = await db.login(username, password);
        db.useBearerAuth(token);
    } catch (err) {
        if (err instanceof arangojs.ArangoError) {
            // ARANGO_NO_AUTH enable
            if (err.code === 404) {
                return true;
            }
        }
        throw err;
    }
    return true;
}

module.exports.getInstance = () => {
    return arangoDbClient;
};

module.exports.connect = async (url, database, username, password = '') => {
    if ((!_.isString(url) && !_.isArray(url)) || !_.isString(database) || !_.isString(username) || !_.isString(password)) {
        throw Error('Invalid arguments');
    }
    let arangoUrl = url;
    if (_.isString(url) && url.indexOf(';') !== -1) {
        arangoUrl = url.split(';').map((item) => item.trim());
    }
    const db = new DatabaseWrapper({
        url: arangoUrl,
        agentOptions: {
            maxSockets: 15,
            keepAlive: true,
            keepAliveMsecs: 10000,
        },
        loadBalancingStrategy: 'ROUND_ROBIN',
    });
    // set module scope arangoDbClient variable to current database object
    arangoDbClient = db;

    db.useDatabase(database);

    await waitDatabaseReady(db);

    await login(db);

    db._revokeTokenTimer = setInterval(() => {
        revokeToken(db);
    }, 1000 * 60 * 60 * 2);

    if (_.isArray(arangoUrl) && arangoUrl.length > 1) {
        // Update all coordinators at startup
        await db.acquireHostList();
        // Updates the URL list by requesting a list of all coordinators in the cluster
        // and adding any endpoints not initially specified in the url configuration.
        // Update it per hour
        db._acquireHostListTimer = setInterval(() => {
            db.acquireHostList();
        }, 1000 * 60 * 60);
    }

    return db;
};

module.exports.aql = arangojs.aql;
module.exports.DocumentCollection = arangojs.DocumentCollection;
module.exports.EdgeCollection = arangojs.EdgeCollection;
module.exports.Graph = arangojs.Graph;
module.exports.Database = arangojs.Database;
module.exports.ArangoError = arangojs.ArangoError;

module.exports.isAqlQuery = Util.isAqlQuery;
module.exports.isGeneratedAqlQuery = Util.isGeneratedAqlQuery;
module.exports.isAqlLiteral = Util.isAqlLiteral;
