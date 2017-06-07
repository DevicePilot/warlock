var crypto       = require('crypto');
var UUID         = require('uuid');

module.exports = function(cache){
  var warlock = {};

  warlock.makeKey = function(key) {
    return key + ':lock';
  };

  /**
   * Set a lock key
   * @param {string}   key    Name for the lock key. String please.
   * @param {integer}  ttl    Time in milliseconds for the lock to live.
   * @param {Function} cb
   */
  warlock.lock = function(key, ttl, cb) {
    cb = cb || function(){};

    if (typeof key !== 'string') {
      return cb(new Error('lock key must be string'));
    }

    var id;
    UUID.v1(null, (id = new Buffer(16)));
    id = id.toString('base64');

    const hash = warlock.makeKey(key);
    try {
      let lockSet = (cache.get(hash) === undefined);
      let unlock = false;

      if (lockSet) {
        lockSet = cache.set(hash, id, ttl/1000 /* ms to seconds */);
        unlock = lockSet ? warlock.unlock.bind(warlock, key, id) : unlock;
      }

      return cb(null, unlock, id);
    } catch(err) {
      return cb(err);
    }

    return key;
  };

  warlock.unlock = function(key, id, cb) {
    cb = cb || function(){};

    if (typeof key !== 'string') {
      return cb(new Error('lock key must be string'));
    }

    const hash = warlock.makeKey(key);
    try {
      const value = cache.get(hash);

      let count = 0;
      if (value === id) {
        count = cache.del(hash);
      }

      return cb(null, count);
    } catch (err) {
      return cb(err);
    }
  };

  /**
   * Set a lock optimistically (retries until reaching maxAttempts).
   */
  warlock.optimistic = function(key, ttl, maxAttempts, wait, cb) {
    var attempts = 0;

    var tryLock = function() {
      attempts += 1;
      warlock.lock(key, ttl, function(err, unlock) {
        if (err) return cb(err);

        if (typeof unlock !== 'function') {
          if (attempts >= maxAttempts) {
            var e = new Error('unable to obtain lock');
            e.maxAttempts = maxAttempts;
            e.key = key;
            e.ttl = ttl;
            e.wait = wait;
            return cb(e);
          }
          return setTimeout(tryLock, wait);
        }

        return cb(err, unlock);
      });
    };

    tryLock();
  };

  return warlock;
};
