var NodeCache = require('node-cache');

var cache = module.exports = new NodeCache();

before(function(done){
  this.cache = new NodeCache();
  return done();
});
