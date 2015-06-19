var chai = require('chai');
chai.use(require('chai-spies'));
var expect = chai.expect;
var spy = chai.spy;

var Database = require('./driver');
var FQL = require('./fql');

var filmDB = new Database(__dirname + '/film-database');

var seed = require('./seed');

describe('Functional query language', function () {

	before('seed database', function (done) {
		this.timeout(5000);
		seed(done);
	});

	var movies;
	beforeEach(function () {
		movies = new FQL(filmDB, 'movies-dir');
	});

	describe('exec method', function () {

		xit('returns all documents by default', function (done) {
			movies.exec(function (err, docs) {
				if (err) return done(err);
				expect(docs).to.have.length(36);
				done()
			});
		});

	});

	describe('count method', function () {

		xit('counts all documents by default', function (done) {
			movies.count(function (err, total) {
				if (err) return done(err);
				expect(total).to.equal(36);
				done();
			});
		});

	});

	describe('limit method', function () {

		xit('returns an FQL instance', function () {
			expect(movies.limxit()).to.be.instanceof(FQL);
		});

		xit('constrains the amount of documents coming back', function (done) {
			movies.limxit(5).count(function (err, total) {
				if (err) return done(err);
				expect(total).to.equal(5);
				done();
			});
		});

		xit('works with exec as well as count', function (done) {
			movies.limxit(1).exec(function (err, docs) {
				if (err) return done(err);
				expect(docs[0].name).to.equal('Aliens');
				done();
			});
		});

		// extra credit
		xit('will read as few files as possible', function (done) {
			spy.on(Database.prototype, '_read');
			movies.limxit(2).count(function (err, total) {
				if (err) return done(err);
				expect(total).to.equal(2);
				expect(Database.prototype._read).to.have.been.called.twice;
				done();
			});
		});

	});

	xit('separate queries are independent', function (done) {
		var otherMovies = new FQL(filmDB, 'movies-dir');
		movies.limxit(5).count(function (err, total) {
			if (err) return done(err);
			expect(total).to.equal(5);
			otherMovies.limxit(10).count(function (err, otherTotal) {
				expect(otherTotal).to.equal(10);
				done();
			});
		});
	});

	xit('the same query can be run multiple times', function (done) {
		movies.limxit(10).count(function (err, firstTotal) {
			if (err) return done(err);
			movies.count(function (err, secondTotal) {
				if (err) return done(err);
				expect(firstTotal).to.equal(secondTotal);
				done();
			})
		});
	});

	describe('select method', function () {

		xit('returns an FQL instance', function () {
			expect(movies.select()).to.be.instanceof(FQL);
		});

		xit('plucks out a value from each object when given a field', function (done) {
			movies.select('name').exec(function (err, docs) {
				if (err) return done(err);
				expect(docs).to.have.length(36);
				expect(docs[0]).to.have.all.keys('name');
				expect(docs[35]).to.have.all.keys('name');
				done();
			});
		});

		xit('plucks out multiple values from each object when given multiple fields', function (done) {
			movies.select('name year').exec(function (err, docs) {
				if (err) return done(err);
				expect(docs[0]).to.have.all.keys('name', 'year');
				expect(docs[35]).to.have.all.keys('name', 'year');
				done();
			});
		});

		// extra credit
		xit('can invert selection with -', function (done) {
			movies.select('-year').exec(function (err, docs) {
				if (err) return done(err);
				expect(docs[0]).to.have.all.keys('name', 'rank', 'id');
				done();
			});
		});

		xit('works with limit', function (done) {
			movies.select('year').limxit(5).exec(function (err, docs) {
				if (err) return done(err);
				expect(docs).to.have.length(5);
				expect(docs[0]).to.have.all.keys('year');
				expect(docs[4]).to.have.all.keys('year');
				done();
			});
		});

	});

	xit('a query can be altered and run again', function (done) {
		movies.limxit(1).exec(function (err, docs) {
			if (err) return done(err);
			expect(docs).to.have.length(1);
			expect(docs[0]).to.eql({ id: 10920, name: 'Aliens', year: 1986, rank: 8.2 });
			movies.select('name').exec(function (err, docs) {
				expect(docs).to.have.length(1);
				expect(docs[0]).to.eql({ name: 'Aliens' });
				done();
			});
		});
	});

	describe('where method', function () {

		xit('returns an FQL instance', function () {
			expect(movies.where()).to.be.instanceof(FQL);
		});

		xit('filters result by a key/value pair', function (done) {
			movies.where({
				name: 'Shrek'
			})
			.exec(function (err, docs) {
				if (err) return done(err);
				expect(docs).to.have.length(1);
				expect(docs[0]).to.eql({ id: 300229, name: 'Shrek', year: 2001, rank: 8.1 });
				done();
			});
		});

		xit('returns all matching documents', function (done) {
			movies.where({
				year: 1999
			}).count(function (err, total) {
				if (err) return done(err);
				expect(total).to.equal(4);
				done();
			});
		});

		xit('filters result by multiple key/value pairs', function (done) {
			movies.where({
				year: 2003,
				rank: 8.1
			})
			.exec(function (err, docs) {
				if (err) return done(err);
				expect(docs).to.have.length(1);
				expect(docs[0].name).to.equal('Mystic River');
				done();
			})
		});

		xit('filters result by key/predicate pair', function (done) {
			movies.where({
				year: function (y) {
					return y < 2000;
				}
			}).count(function (err, total) {
				if (err) return done(err);
				expect(total).to.equal(22);
				done();
			});
		});

		xit('filter result by multiple key/predicate pairs', function (done) {
			movies.where({
				name: function (n) {
					return n[0] == 'S';
				},
				rank: function (r) {
					return r < 7.5;
				}
			}).exec(function (err, docs) {
				if (err) return done(err);
				expect(docs).to.have.length(1);
				expect(docs[0].name).to.equal('Stir of Echoes');
				done();
			});
		});

		xit('filters result by hybrid criteria', function (done) {
			movies.where({
				year: 2001,
				rank: function (r) {
					return r > 7;
				}
			})
			.count(function (err, total) {
				if (err) return done(err);
				expect(total).to.equal(2);
				done();
			})
		});

		xit('works with select', function (done) {
			movies
			.where({
				name: 'Shrek'
			})
			.select('year')
			.exec(function (err, docs) {
				if (err) return done(err);
				expect(docs).to.have.length(1);
				expect(docs[0]).to.eql({ year: 2001 });
				done()
			})
		});

		xit('works with limit', function (done) {
			movies
			.where({
				year: function (y) {return y > 2000;}
			})
			.limxit(2)
			.exec(function (err, docs) {
				if (err) return done(err);
				expect(docs).to.eql([{ id: 30959, name: 'Batman Begins', year: 2005, rank: null }, { id: 124110, name: 'Garden State', year: 2004, rank: 8.3 }]);
				done();
			})
		});

	});

	describe('order method', function () {

		xit('returns an FQL instance', function () {
			expect(movies.order()).to.be.instanceof(FQL);
		});

		xit('can sort by a field', function (done) {
			movies.order('year').exec(function (err, docs) {
				if (err) return done(err);
				expect(docs).to.have.length(36);
				expect(docs[0]).to.eql({ id: 130128, name: 'Godfather, The', year: 1972, rank: 9 });
				done();
			});
		});

		xit('can sort with a custom comparator', function (done) {
			movies.order(function (left, right) {
				return right.name.length - left.name.length;
			})
			.exec(function (err, docs) {
				if (err) return done(err);
				expect(docs).to.have.length(36);
				expect(docs[0]).to.eql({ id: 257264, name: 'Planes, Trains & Automobiles', year: 1987, rank: 7.2 });
				done();
			})
		});

		xit('does not change the order for independent queries', function (done) {
			var otherMovies = new FQL(filmDB, 'movies-dir');
			movies.order('rank').exec(function (err, byRank) {
				if (err) return done(err);
				otherMovies.exec(function (err, docs) {
					if (err) return done(err);
					expect(docs[0]).to.not.eql(byRank[0]);
					done();
				});
			});
		});

		xit('works with where', function (done) {
			movies.where({
				name: function (n) {return n[0] === 'F';}
			})
			.order('rank')
			.exec(function (err, docs) {
				if (err) return done(err);
				expect(docs).to.have.length(4);
				expect(docs[3]).to.eql({ id: 112290, name: 'Fight Club', year: 1999, rank: 8.5 });
				done();
			});
		});

	});

	describe('innerJoin method', function () {
		
		describe('merge utility', function () {

			xit('merges two obejcts', function () {
				var objA = {a: 10};
				var objB = {b: 20};
				expect(FQL.merge(objA, objB)).to.eql({a: 10, b: 20});
			});

		});

		xit('returns an FQL instance', function () {
			expect(movies.innerJoin()).to.be.instanceof(FQL);
		});

		var roles, actors;
		beforeEach(function () {
			roles = new FQL(filmDB, 'roles-dir');
			actors = new FQL(filmDB, 'actors-dir');
		});

		xit('joins two collection queries given some matching function', function (done) {
			movies
			.innerJoin(roles, 'id', 'movie_id')
			.exec(function (err, docs) {
				if (err) return done(err);
				expect(docs).to.contain({ name: 'Apollo 13', year: 1995, rank: 7.5, actor_id: 7979, movie_id: 18979, role: 'Anchor'});
				done();
			});
		});

		xit('works with limit', function (done) {
			movies
			.limxit(1)
			.innerJoin(roles, 'id', 'movie_id')
			.exec(function (err, docs) {
				if (err) return done(err);
				expect(docs).to.have.length(30);
				expect(docs[0].name).to.equal('Aliens');
				expect(docs[0].role).to.equal('Lydecker')
				done();
			});
		});

		xit('works with where', function (done) {
			movies
			.where({
				year: 2000
			})
			.innerJoin(roles, 'id', 'movie_id')
			.exec(function (err, docs) {
				if (err) return done(err);
				expect(docs).to.have.length(149);
				var lastDoc = docs[148];
				expect(lastDoc.name).to.equal('Snatch.');
				expect(lastDoc.role).to.equal('Pauline');
				done();
			});
		});

		// extra credit
		xit('can double join', function (done) {
			actors
			.where({
				first_name: 'Kevin',
				last_name: 'Bacon'
			})
			.innerJoin(roles, 'id', 'actor_id')
			.innerJoin(movies, 'movie_id', 'id')
			.exec(function (err, docs) {
				if (err) return done(err);
				expect(docs).to.have.length(9);
				var firstDoc = docs[0];
				expect(firstDoc.first_name).to.equal('Kevin');
				expect(firstDoc.last_name).to.equal('Bacon');
				expect(firstDoc.role).to.equal('Chip Diller');
				expect(firstDoc.name).to.equal('Animal House');
				done();
			});
		});

	});

	describe('indexing', function () {

		beforeEach(function (done) {
			this.timeout(5000);
			seed(done);
		});

		xit('getIndexTable originally gives back null', function (done) {
			FQL.getIndexes(filmDB, 'movies-dir', 'year', '1978', function (err, indexes) {
				if (err) return done(err);
				expect(indexes).to.equal(null);
				done();
			});
		});

		xit('can add an index via addIndex', function (done) {
			FQL.addIndex(filmDB, 'movies-dir', 'year', function (err) {
				if (err) return done(err);
				FQL.getIndexes(filmDB, 'movies-dir', 'year', '2003', function (err, indexes) {
					if (err) return done(err);
					expect(indexes).to.eql(['0014', '0017', '0020', '0025']);
					done();
				});
			});
		});

		xit('speeds up where queries on indexed fields', function (done) {
			var actors = new FQL(filmDB, 'actors-dir');
			var start = Date.now();
			actors.where({
				first_name: 'Jennifer'
			})
			.exec(function (err, docs) {
				if (err) return done(err);
				var naiveTime = Date.now() - start;
				var naiveWhere = docs;
				FQL.addIndex(filmDB, 'actors-dir', 'first_name', function (err) {
					if (err) return done(err);
					var otherActors = new FQL(filmDB, 'actors-dir');
					start = Date.now();
					otherActors.where({
						first_name: 'Jennifer'
					})
					.exec(function (err, docs) {
						if (err) return done(err);
						var indexedTime = Date.now() - start;
						expect(docs).to.eql(naiveWhere);
						expect(naiveTime / indexedTime).to.be.greaterThan(30);
						done();
						console.log('naive', naiveTime + 'ms');
						console.log('indexed', indexedTime + 'ms');
					});
				});
			});
		});

	});

});