'use strict';

function FQL (db, collectionName) {
	this.db = db;
	this.collectionName = collectionName;
	this.keys = undefined;
	return this;
}

FQL.prototype.exec = function (cb) {
	var i = 0;
	var limit = this.limit;
	var limiterator = function () {
		i++;
		return i > limit;
	}
	var selection = this.selection;
	var selecterator = function () {
		done(false);
	}
	if(!this.limit && !this.selection){
		this.db.findAll(this.collectionName, cb);
	}else if(!this.limit){
		this.db.filterAll(this.collectionName, selecterator, cb)
	}else if(!this.selection){
		this.db.findUntil(this.collectionName, limiterator, cb);
	}else{
		
	}
	return this;	
};

FQL.prototype.count = function (cb) {
	var countCallback = function(err, docs){
			if (err) { return console.log(err) }
			cb(null, docs.length);
		}
	this.exec(countCallback);
	
	return this;
};

FQL.prototype.limit = function (amount) {
	this.limit = amount;
	return this;
};

FQL.prototype.select = function (str) {
	if(str){
		this.invertSelect = str[0] === "-" ? true : false;
		this.selection = str.replace("-", "").split(" ");
	}
	return this;
};

FQL.prototype.where = function (criteria) {};

FQL.prototype.order = function (fieldOrComparator) {};

FQL.merge = function (obj1, obj2) {};

FQL.prototype.innerJoin = function (foreignFQL, field, foreignField) {};

FQL.getIndexes = function (db, collectionName, field, value, cb) {};

FQL.addIndex = function (db, collectionName, field, cb) {};

module.exports = FQL;