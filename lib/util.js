function isAqlQuery(query) {
    return Boolean(query && query.query && query.bindVars);
}
module.exports.isAqlQuery = isAqlQuery;

function isGeneratedAqlQuery(query) {
    return isAqlQuery(query) && typeof query._source === 'function';
}
module.exports.isGeneratedAqlQuery = isGeneratedAqlQuery;

function isAqlLiteral(literal) {
    return Boolean(literal && typeof literal.toAQL === 'function');
}
module.exports.isAqlLiteral = isAqlLiteral;

