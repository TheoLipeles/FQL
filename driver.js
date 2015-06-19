'use strict';

var fsExtra = require('fs-extra'),
	path = require('path'),
	async = require('async');

function toObject (doc) {
	return JSON.parse(doc);
}

function toDocument (obj) {
	return JSON.stringify(obj, null, 4);
}

function toFourDigitString (num) {
	return (num + 10000).toString().slice(-4);
}

function keyToFilename (key) {
	if (typeof key === 'number') key = toFourDigitString(key);
	return key + '.json';
}

function fileNameToKey (filename) {
	return path.basename(filename, '.json');
}

function readAndParse (filepath, cb) {
	fsExtra.readFile(filepath, function (err, contents) {
		if (err) return cb(err);
		var obj = toObject(contents);
		cb(null, obj);
	});
}

function stringifyAndWrite (filepath, obj, cb) {
	var doc = toDocument(obj);
	fsExtra.writeFile(filepath, doc, function (err) {
		cb(err, obj);
	});
}

function Database (dbPath) {
	this.dbPath = dbPath;
	this._maxKeys = {};
	this.initialize();
}

Database.prototype.ensureDB = function () {
	fsExtra.ensureDirSync(this.dbPath);
};

Database.prototype.ensureCollection = function (collectionName) {
	var collectionPath = path.join(this.dbPath, collectionName);
	fsExtra.ensureDirSync(collectionPath);
};

Database.prototype.listCollections = function () {
	return fsExtra.readdirSync(this.dbPath);
};

Database.prototype.listKeys = function (collectionName) {
	this.ensureCollection(collectionName);
	var dirPath = path.join(this.dbPath, collectionName);
	return fsExtra.readdirSync(dirPath).map(fileNameToKey);
};

Database.prototype.initialize = function () {
	this.ensureDB();
	var db = this;
	this.listCollections().forEach(function (collectionName) {
		var keys = db.listKeys(collectionName);
		var maxKey = parseInt(keys[keys.length-1] || 0);
		db._maxKeys[collectionName] = maxKey;
	});
};

Database.prototype.nextKey = function (collectionName) {
	return this._maxKeys[collectionName] = (this._maxKeys[collectionName] || 0) + 1;
};

Database.prototype._read = function (collectionName, filename, cb) {
	this.ensureCollection(collectionName);
	var filepath = path.join(this.dbPath, collectionName, filename);
	readAndParse(filepath, cb);
};

Database.prototype.find = function (collectionName, key, cb) {
	var filename = keyToFilename(key);
	this._read(collectionName, filename, cb);
};

Database.prototype.findAll = function (collectionName, cb) {
	var keys = this.listKeys(collectionName);
	var onEach = this.find.bind(this, collectionName);
	async.map(keys, onEach, cb);
};

Database.prototype.findUntil = function (collectionName, condition, cb) {
	var keys = this.listKeys(collectionName);
	var db = this;
	function iterator (objs, key, next) {
		if (condition(objs)) return cb(null, objs);
		db.find(collectionName, key, function (err, obj) {
			if (err) return next(err);
			objs.push(obj);
			next(null, objs);
		});
	}
	async.reduce(keys, [], iterator, cb);
};

Database.prototype.filterAll = function (collectionName, predicate, cb) {
	var keys = this.listKeys(collectionName);
	var db = this;
	function onEach (key, done) {
		db.find(collectionName, key, function (err, obj) {
			if (err) done(false);
			else done(predicate(obj));
		});
	}
	async.filter(keys, onEach, cb);
};

Database.prototype.filterUntil = function (collectionName, predicate, condition, cb) {
	var keys = this.listKeys(collectionName);
	var db = this;
	function iterator (objs, key, next) {
		if (condition(objs)) return cb(null, objs);
		db.find(collectionName, key, function (err, obj) {
			if (err) return next(err);
			if (predicate(obj)) objs.push(obj);
			next(null, objs);
		});
	}
	async.reduce(keys, [], iterator, cb);
};

Database.prototype._write = function (collectionName, filename, obj, cb) {
	this.ensureCollection(collectionName);
	var filepath = path.join(this.dbPath, collectionName, filename);
	stringifyAndWrite(filepath, obj, cb);
};

Database.prototype.insert = function (collectionName, obj, cb) {
	var key = this.nextKey(collectionName);
	var filename = keyToFilename(key);
	this._write(collectionName, filename, obj, cb);
};

Database.prototype.insertAll = function (collectionName, objs, cb) {
	var onEach = this.insert.bind(this, collectionName);
	async.map(objs, onEach, cb);
};

Database.prototype.update = function (collectionName, key, obj, cb) {
	var filename = keyToFilename(key);
	this._write(collectionName, filename, obj, cb);
};

Database.prototype._delete = function (collectionName, filename, cb) {
	this.ensureCollection(collectionName);
	var filepath = path.join(this.dbPath, collectionName, filename);
	fsExtra.unlink(filepath, cb);
};

Database.prototype.remove = function (collectionName, key, cb) {
	var db = this;
	this.find(collectionName, key, function (err, obj) {
		if (err) return cb(err);
		var filename = keyToFilename(key);
		db._delete(collectionName, filename, function (err) {
			cb(err, obj);
		});
	});
};

Database.prototype.removeAll = function (collectionName, cb) {
	var keys = this.listKeys(collectionName);
	var onEach = this.remove.bind(this, collectionName);
	async.map(keys, onEach, cb);
};

Database.prototype.drop = function (cb) {
	var collections = this.listCollections();
	var db = this;
	function onEach (collectionName, done) {
		var collectionPath = path.join(db.dbPath, collectionName);
		fsExtra.remove(collectionPath, done);
	}
	function onComplete (err) {
		if (err) return cb(err);
		db._maxKeys = {};
		cb(null);
	}
	async.each(collections, onEach, onComplete);
};

Database.prototype.Collection = function (name) {
	var collection = new Collection(this, name);
	return collection;
};

function Collection (db, name) {
	this.db = db;
	this.name = name;
}

Collection.assumeName = function (methodName) {
	this.prototype[methodName] = function () {
		Database.prototype[methodName].bind(this.db, this.name).apply(this.db, arguments);
	}
};

['find', 'findUntil', 'findAll', 'insert', 'insertAll', 'filterAll', 'filterUntil', 'update', 'remove', 'removeAll', 'listKeys'].forEach(function (methodName) {
	Collection.assumeName(methodName);
});

module.exports = Database;