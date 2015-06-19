'use strict';

function FQL (db, collectionName) {}

FQL.prototype.exec = function (cb) {};

FQL.prototype.count = function (cb) {};

FQL.prototype.limit = function (amount) {};

FQL.prototype.select = function (str) {};

FQL.prototype.where = function (criteria) {};

FQL.prototype.order = function (fieldOrComparator) {};

FQL.merge = function (obj1, obj2) {};

FQL.prototype.innerJoin = function (foreignFQL, field, foreignField) {};

FQL.getIndexes = function (db, collectionName, field, value, cb) {};

FQL.addIndex = function (db, collectionName, field, cb) {};

module.exports = FQL;