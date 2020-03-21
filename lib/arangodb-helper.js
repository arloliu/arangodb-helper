const _ = require('lodash');
const arangojs = require('arangojs');
const aql = arangojs.aql;
const {isGeneratedAqlQuery} = require('./util');

/*
    obj = {
        field: 'asc_num' // asc by number forcelly
        field: 'asc' // asc by original type
        field: 'desc_num' // asc by number forcelly
        field: 'desc' // desc by original type
        field: true // ASC
        field: false // DESC
    }
*/
function parseSort(obj, documentName) {
    if (!_.isPlainObject(obj)) {
        return aql.literal('');
        // throw new arangojs.ArangoError('Invalid SORT parameter');
    }
    const doc = documentName ? documentName : 'doc';

    const literals = [];
    for (const field in obj) {
        if (!Object.hasOwnProperty.call(obj, field)) {
            continue;
        }

        let direction;
        let fieldLiteral;
        if (_.isBoolean(obj[field])) {
            direction = obj[field] ? 'ASC' : 'DESC';
            fieldLiteral = aql.literal(`${doc}.${field} ${direction}`);
        } else if (_.isString(obj[field])) {
            const data = obj[field].toLowerCase().trim();
            if (data === 'asc_num' || data === 'asc') {
                direction = 'ASC';
            } else if (data === 'desc_num' || data === 'desc') {
                direction = 'DESC';
            }

            if (data === 'asc_num' || data === 'desc_num') {
                fieldLiteral = aql.literal(`TO_NUMBER(${doc}.${field}) ${direction}`);
            } else {
                fieldLiteral = aql.literal(`${doc}.${field} ${direction}`);
            }
        }
        literals.push(fieldLiteral);
    }

    return aql`SORT ${aql.join(literals, ', ')}`;
}

function parseLimit(obj) {
    if (!_.isPlainObject(obj) ||
        !_.isNumber(obj.offset) ||
        !_.isNumber(obj.count)
    ) {
        throw new arangojs.ArangoError('Invalid LIMIT parameter');
    }

    return aql`LIMIT ${obj.offset}, ${obj.count}`;
}

function buildReturnFields(fields, documentName) {
    const doc = documentName ? documentName : 'doc';
    if (!_.isArray(fields) || fields.length < 1) {
        return aql.literal(doc);
    }
    const returnFields = [];
    fields.forEach((field) => {
        returnFields.push(`${field}: ${doc}.${field}`);
    });

    return aql.literal('{' + returnFields.join(',') + '}');
}

function parseFilter(value) {
    if (isGeneratedAqlQuery(value)) {
        return aql`FILTER ${value}`;
    } else if (_.isPlainObject(value)) {
        const filters = [];
        _.forIn(value, (val, key) => {
            filters.push(aql`doc[${key}] == ${val}`);
        });
        return aql`FILTER ${aql.join(filters, ' AND ')}`;
    }
    return aql.literal('');
}

function parseNewValue(value) {
    if (isGeneratedAqlQuery(value)) {
        return value;
    } else if (_.isPlainObject(value)) {
        const literals = [];
        _.forIn(value, (val, key) => {
            let aqlValue;
            if (_.isNumber(val)) {
                aqlValue = val;
            } else if (_.isString(val)) {
                aqlValue = `"${val}"`;
            }
            literals.push(aql.literal(`${key}: ${aqlValue}`));
        });
        return aql.join(literals, ', ');
    }
    return null;
}
const OperatorMap = {
    'eq': '==',
    'neq': '!=',
    'lt': '<',
    'lte': '<=',
    'gt': '>',
    'gte': '>=',
    'in': 'IN',
    'like': 'LIKE',
};

class ArangoDbHelper {
    constructor(database, collectionName) {
        this.db = database;
        this.collection = this.db.collection(collectionName);
        this.collectionName = collectionName;
    }

    getReturnFields(fields, documentName) {
        return buildReturnFields(field, documentName);
    }

    getAqlLimit(offset, count) {
        let aqlLimit;
        if (_.isInteger(offset) && _.isInteger(count)) {
            aqlLimit = aql`LIMIT ${offset}, ${count}`;
        } else {
            aqlLimit = aql.literal('');
        }
        return aqlLimit;
    }

    getAqlSort(sort, documentName) {
        return parseSort(sort, documentName);
    }

    getFieldOperator(opKey) {
        const lowerCaseKey = opKey.toLowerCase();
        if (!OperatorMap.hasOwnProperty(lowerCaseKey)) {
            throw new SyntaxError(`Invalid operator expression, operator '${lowerCaseKey}' invalid`);
        }
        const opValue = OperatorMap[lowerCaseKey];
        return opValue;
    }

    getFieldFilter(fields, documentName) {
        const doc = documentName ? documentName : 'doc';
        const filters = [];
        for (const name in fields) {
            if (!fields.hasOwnProperty(name)) {
                continue;
            }
            const field = fields[name];
            const fieldKeys = Object.keys(field);

            if (!fieldKeys || fieldKeys.length < 1 || !_.isString(fieldKeys[0])) {
                throw new SyntaxError('Invalid operator expression');
            }

            fieldKeys.forEach((fieldKey) => {
                const opValue = this.getFieldOperator(fieldKey);
                filters.push(aql`${aql.literal(`${doc}.${name} ${opValue}`)} ${field[fieldKey]}`);
            });
        }
        if (filters.length < 1) {
            return aql.literal('');
        }

        return aql`FILTER ${aql.join(filters, ' AND ')}`;
    }

    async create(properties) {
        return await this.collection.create(properties);
    }

    async setProperties(properties) {
        return await this.collection.setProperties(properties);
    }

    async load(count) {
        return await this.collection.load(count);
    }

    async unload() {
        return await this.collection.unload();
    }

    async rename(name) {
        return await this.collection.rename(name);
    }

    async rotate() {
        return await this.collection.rotate();
    }

    async truncate(properties) {
        return await this.collection.truncate(properties);
    }

    async drop() {
        return await this.collection.drop();
    }

    async import(data, opts) {
        return await this.collection.import(data, opts);
    }

    async save(user) {
        const result = await this.collection.save(user, {returnNew: true});
        return result.new;
    }

    async update(documentHandle, newValue, options) {
        const updateOptions = _.merge({returnNew: true}, options);
        const result = await this.collection.update(documentHandle, newValue, updateOptions);
        return result.new;
    }

    async remove(documentHandle) {
        await this.collection.remove(documentHandle);
        const exists = await this.collection.documentExists(documentHandle);
        return !exists;
    }

    async findOneAndUpdate(filter, newValue) {
        const result = await this.findOne(filter);
        if (result) {
            return this.update(result, newValue);
        }
        return null;
    }

    async findAndUpdate(filter, newValue) {
        const aqlFilter = parseFilter(filter);
        const aqlNewValue = parseNewValue(newValue);

        if (!aqlNewValue) {
            throw new SyntaxError('Invalid new value in findAndUpdate method');
        }
        const aqlCollection = aql.literal(this.collectionName);
        const query = aql`
            FOR doc IN ${aqlCollection}
            ${aqlFilter}
            UPDATE doc WITH {${aqlNewValue}} IN ${aqlCollection}
            RETURN NEW
        `;

        const cursor = await this.db.query(query);
        if (!cursor.hasNext()) {
            return null;
        }
        return await cursor.next();
    }

    async find(filter, sort, limit, fields) {
        const aqlFilter = parseFilter(filter);
        const aqlSort = _.isPlainObject(sort) ? parseSort(sort) : aql.literal('');
        const aqlLimit = _.isPlainObject(limit) ? parseLimit(limit) : aql.literal('');
        const aqlCollection = aql.literal(this.collectionName);
        const returnFields = buildReturnFields(fields);
        const query = aql`
            FOR doc IN ${aqlCollection}
            ${aqlFilter}
            ${aqlSort}
            ${aqlLimit}
            RETURN ${returnFields}
        `;

        return await this.db.query(query);
    }

    async findOne(filter, fields) {
        const cursor = await this.find(filter, null, null, fields);
        if (!cursor.hasNext()) {
            return null;
        }
        return await cursor.next();
    }

    async findAll(filter, sort, limit, fields) {
        const cursor = await this.find(filter, sort, limit, fields);
        if (!cursor.hasNext()) {
            return [];
        }
        return await cursor.all();
    }

    async pushItemIntoArray(key, field, item) {
        const aqlCollection = aql.literal(this.collectionName);
        const aqlFilter = parseFilter({_key: key});
        const aqlField = aql.literal(field);
        const query = aql`
            FOR doc IN ${aqlCollection}
                ${aqlFilter}
                UPDATE doc WITH {
                    ${aqlField}: PUSH(doc.${aqlField}, ${item})
                } IN ${aqlCollection}
            RETURN NEW
        `;

        const cursor = await this.db.query(query);
        if (!cursor.hasNext()) {
            return null;
        }
        return await cursor.next();
    }

    async removeItemInArray(key, field, position) {
        const aqlCollection = aql.literal(this.collectionName);
        const aqlFilter = parseFilter({_key: key});
        const aqlField = aql.literal(field);
        const query = aql`
            FOR doc IN ${aqlCollection}
                ${aqlFilter}
                UPDATE doc WITH {
                    ${aqlField}: REMOVE_NTH(doc.${aqlField}, ${position})
                } IN ${aqlCollection}
            RETURN NEW
        `;

        const cursor = await this.db.query(query);
        if (!cursor.hasNext()) {
            return null;
        }
        return await cursor.next();
    }
};

module.exports = ArangoDbHelper;
