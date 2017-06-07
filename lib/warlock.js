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

    redis.set(
      warlock.makeKey(key), // key name
      id, // key value
      'PX', ttl, // Set the specified expire time (ttl), in milliseconds.
      'NX', // Only set the key if it does not already exist.
      
      function( // callback
        err, // probably not a thing
        lockSet // 0K if SET, null if NX blocks set.
        ) {
        if (err) return cb(err);

        var unlock = warlock.unlock.bind(warlock, key, id);
        if (!lockSet) unlock = false;

        return cb(err, unlock, id);
      }
    );

    return key;
  };

  warlock.unlock = function(key, id, cb) {
    cb = cb || function(){};

    if (typeof key !== 'string') {
      return cb(new Error('lock key must be string'));
    }

    /* redis */scripty.loadScriptFile(
      'parityDel',
      __dirname + '/lua/parityDel.lua',
      /*
        --
        -- Delete a key if content is equal
        --
        -- KEYS[1]   - key
        -- KEYS[2]   - content
        local key     = KEYS[1]
        local content = ARGV[1]

        local value = redis.call('get', key)

        if value == content then
          return redis.call('del', key); // Return # keys deleted; e.g. 1.
        end

        return 0
      */
      function(err, parityDel){
        if (err) return cb(err);

        return parityDel.run(
          1, // number; ignore.
          warlock.makeKey(key), // KEYS[1]
          id, // ARGV[1]
          cb // fn(err, return);
        );
      }
    );
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
