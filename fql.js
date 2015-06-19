'use strict';

function FQL (db, collectionName) {
	this.db = db;
	this.collectionName = collectionName;
	this.keys = undefined;
	return this;
}

FQL.prototype.exec = function (cb) {
	if(!this.limit){
		this.db.findAll(this.collectionName, cb);
	}else{
		var i = 0;
		var limit = this.limit;
		var limiterator = function () {
			console.log(i);
			i++;
			return i > limit;
		}
		this.db.findUntil(this.collectionName, limiterator, cb);
	}
	return this;	
};

FQL.prototype.count = function (cb) {
	var countCallback = function(err, docs){
			if (err) { return console.log(err) }
			cb(null, docs.length);
		}
	if(!this.limit){
		this.db.findAll(this.collectionName, countCallback);
	}else{
		var i = 0;
		var limit = this.limit;
		var limiterator = function () {
			console.log(i);
			i++;
			return i > limit;
		}
		this.db.findUntil(this.collectionName, limiterator, countCallback);
	}
	
	return this;
};

FQL.prototype.limit = function (amount) {
	this.limit = amount;
	return this;
};

FQL.prototype.select = function (str) {};

FQL.prototype.where = function (criteria) {};

FQL.prototype.order = function (fieldOrComparator) {};

FQL.merge = function (obj1, obj2) {};

FQL.prototype.innerJoin = function (foreignFQL, field, foreignField) {};

FQL.getIndexes = function (db, collectionName, field, value, cb) {};

FQL.addIndex = function (db, collectionName, field, cb) {};

module.exports = FQL;