'use strict';

var asyncMap = require('slide').asyncMap;
var Id = require('./id.js');
var RoutingTable = require('./routing-table.js');
var Lookup = require('./lookup.js');
var Contact = require('./contact.js');

var RPC_FUNCTIONS = ['ping', 'store', 'findNode', 'findValue', 'receive'];

// Check that an object possesses the specified functions.
//
function checkInterface(obj, funcs) {
    for (var i = 0; i < funcs.length; ++i) {
        if (typeof obj[funcs[i]] !== 'function')
            return false;
    }
    return true;
}

//
// 2. Network Characterization
// ===========================

// Fill `opts` with the default options if needed.
//
function defaultOptions(opts) {
    opts.bucketSize  = opts.bucketSize  || 20; // k
    opts.concurrency = opts.concurrency || 3;  // alpha
    opts.expireTimeMiliseconds    = opts.expireTimeMiliseconds    || 1000 * (60 * 60 * 24 + 10);
    opts.refreshTimeMiliseconds   = opts.refreshTimeMiliseconds   || 1000 * (60 * 60);
    opts.replicateTimeMiliseconds = opts.replicateTimeMiliseconds || 1000 * (60 * 60);
    opts.republishTimeMiliseconds = opts.republishTimeMiliseconds || 1000 * (60 * 60 * 24);
}

//
// 3. The Node
// ===========

// Store key/value pairs on a distributed network. `rpc` must provide the
// necessary Kademlia RPC methods for the local node of the DHT.
//
var Dht = function (rpc, id, opts) {
    if (!checkInterface(rpc, RPC_FUNCTIONS))
        throw new Error('the RPC interface is not fully defined');
    rpc.receive('ping', this._onPing.bind(this));
    rpc.receive('store', this._onStore.bind(this));
    rpc.receive('findNode', this._onFindNode.bind(this));
    rpc.receive('findValue', this._onFindValue.bind(this));
    Object.defineProperty(this, 'rpc', {value: rpc});
    this.id = id;
    this._cache = {};
    this._routes = new RoutingTable(id, opts.bucketSize);
    this._opts = opts;
    this._pendingContact = null;
    this._lookupOpts = {
        size: opts.bucketSize,
        concurrency: opts.concurrency,
        findNode: this._findNodeOrValue.bind(this),
        findNodeOrValue: this._findNodeOrValue.bind(this)
    };

    var self = this;
    replicateLoop();

    function replicateLoop(){
      var now = new Date();
      var nextTime = self._replicateCache(now);
      setTimeout(replicateLoop, nextTime - now);
    }
};

// Create a Dht instance with a random ID.
//
Dht.spawn = function (rpc, seeds, opts, cb) {
    if (typeof cb === 'undefined') {
        cb = opts;
        opts = {};
    }
    defaultOptions(opts);
    // A node joins the network as follows: (§4.7, Join)
    // 1. if it does not already have a nodeID n, it generates one
    Id.generate(function onGotDhtId(err, id) {
        if (err) return cb(err);
        var dht = new Dht(rpc, id, opts);
        dht.bootstrap(seeds, function (err) {
            cb(null, dht);
        });
    });
};

Dht.prototype.close = function () {
    this.rpc.close();
};

Dht.prototype.getSeeds = function () {
    return this._routes.find(this._routes.id);
};

Dht.prototype.getCache = function () {
    this._expireCache();
    return this._cache;
};

Dht.prototype._expireCache = function (now) {
    // All key/value pairs are assigned an expiration time which is
    // "exponentially inversely proportional to the number of nodes
    // between the current node and the node whose ID is closest to
    // the key", where this number is "inferred from the bucket
    // structure of the current node". (§4.9)
    now = now || new Date();
    for(var idkey in this._cache) {
        var id = Id.fromHex(idkey);
        var entry = this._cache[idkey];
        var numNodes = this._routes.countClosestNodes(id);
        var expireFactor = (numNodes > this._opts.bucketSize) ?
            Math.exp(this._opts.bucketSize / numNodes) : 1;
        for(var subkey in entry) {
            var expire = entry[subkey].expire;
            if(!expire) continue;
            if(expireFactor != 1) {
                expire = (expire - this._opts.expireTimeMiliseconds) +
                         (expire - this._opts.expireTimeMiliseconds) * expireFactor;
            }
            if(expire < now) {
              console.log(idkey + " " + subkey + " expired at: " + expire);
              delete entry[subkey];
            }
        }
    }
};

// Replicate values in cache when necessary and return the date of next
// replication.
//
Dht.prototype._replicateCache = function (now) {
    // Each node republishes each key/value pair that it contains at
    // intervals of tReplicate (§4.8)
    // The original publisher of a key/value pair republishes it every
    // tRepublish (§4.8)
    now = now || new Date();
    var nextReplicate = now + this._opts.replicateTimeMiliseconds;
    this._expireCache();
    for(var idkey in this._cache) {
        var id = Id.fromHex(idkey);
        for(var subkey in this._cache[idkey]) {
            var entry = this._cache[idkey][subkey];
            var replicate = entry.refresh + this._opts.replicateTimeMiliseconds;
            if(replicate < now) {
                this._replicate(idkey, subkey);
                entry.refresh = now;
            } else if(replicate < nextReplicate) {
                nextReplicate = replicate;
            }
        }
    }
}

Dht.prototype.bootstrap = function (seeds, cb) {
    if (seeds.length === 0)
        return process.nextTick(function () {
            return cb();
        });
    var self = this;
    var payload = {id: this._routes.id};
    payload.targetId = payload.id;
    var remain = seeds.length;
    for (var i = 0; i < seeds.length; ++i) {
        this.rpc.ping(seeds[i], payload, bootstrapSome.bind(null, seeds[i]));
    }
    refreshLoop();

    function bootstrapSome(endpoint, err, res) {
        --remain;
        if (err) {
            if (remain === 0) return self._bootstrapLookup(cb);
            return;
        }
        // A node joins the network as follows: (§4.7, Join)
        // 2. it inserts the value of some known node c into the appropriate
        //    bucket as its first contact
        // 3. it does an iterativeFindNode for n [the current node id]
        var contact = new Contact(res.remoteId, endpoint);
        self._routes.store(contact);
        if (remain === 0) {
            self.iterativeFindNode(self._routes.id, function(err, contacts){
                return cb();
            });
        }
    }

    function refreshLoop(){
        // A node joins the network as follows: (§4.7, Join)
        // 4. it refreshes all buckets further away than its closest neighbor, which
        //    will be in the occupied bucket with the lowest index.
        var nextTime = self._routes.refresh(self, self._opts.refreshTimeMiliseconds);
        var now = new Date();
        setTimeout(refreshLoop, Math.max(0, nextTime - now));
    }
};

Dht.prototype.iterativeFindNode = function (id, cb) {
    if(!(id instanceof Id)) throw new Error('invalid id');
    var seeds = this._routes.find(id, this._opts.concurrency);
    this._routes.markRefreshed(id);
    Lookup.proceed(id, seeds, this._lookupOpts, function (err, contacts) {
        return cb(err, contacts, id);
    });
};

// Set a key/value pair.
//
Dht.prototype.set = function (key, value, cb) {
  return this.multiset(key, key, value, cb);
}

// Set a key-subkey/value
//
Dht.prototype.multiset = function (key, subkey, value, cb) {
    var keyid = (key instanceof Id) ? key : Id.fromKey(key);
    var self = this;
    // This is the Kademlia store operation. The initiating node does an
    // iterativeFindNode, collecting a set of k closest contacts, and then
    // sends a primitive STORE RPC to each. (§4.5.2)
    this.iterativeFindNode(keyid, function (err, contacts, id) {
        if (err) return cb(err);
        self._storeToMany(keyid.toString(), subkey, value, contacts, null, cb);
    });
};

// Replicate a key-subkey/value
//
Dht.prototype._replicate = function (idkey, subkey) {
    var self = this;
    var entry = this._cache[idkey][subkey];
    this.iterativeFindNode(Id.fromHex(idkey), function (err, contacts, id) {
        if (err) return;
        var now = new Date();
        self._storeToMany(idkey, subkey, entry.value, contacts, entry.expire - now);
    });
};

// Store the key/value pair into the specified contacts.
//
Dht.prototype._storeToMany = function (idkey, subkey, value, contacts, expireTimeout, cb) {
    var self = this;
    asyncMap(contacts, function (contact, cb) {
        self._storeTo(idkey, subkey, value, contact, expireTimeout, cb);
    }, cb);
};

// Store a key/pair into the specified contact.
//
// TODO @jeanlauliac What to do if we get an error? Remove the contact from
// the routing table? May be better to give it a second chance later.
//
Dht.prototype._storeTo = function (idkey, subkey, value, contact, expireTimeout, cb) {
    if (contact.id.equal(this._routes.id)) {
        this._storeToCache(idkey, subkey, value, expireTimeout);
        return cb ? process.nextTick(cb) : undefined;
    }
    // All key/value pairs expire tExpire secondsafter the original
    // publication (§4.9)
    var payload = {
        id:     this._routes.id,
        idkey:  idkey,
        subkey: subkey,
        value:  value,
        expire: expireTimeout || this._opts.expireTimeMiliseconds
    };
    this.rpc.store(contact.endpoint, payload, function (err, result) {
        return cb ? cb() : undefined;
    });
};

// Store value in cache
//
Dht.prototype._storeToCache = function (idkey, subkey, value, expireTimeout) {
    if(!this._cache[idkey]) this._cache[idkey] = {};
    var now = new Date();
    if(subkey instanceof Array) {
        for(var i = 0; i < subkey.length; ++i) {
            var subk = subkey[i];
            var expire = null;
            if(!expireTimeout) {
                expire = null;
            } else if(typeof expireTimeout == 'number') {
                expire = now + expireTimeout;
            } else {
                expire = now + expireTimeout[subk];
            }
            this._cache[idkey][subk] = {value: value[subk], expire: expire, refresh: now};
        }
    } else {
        var expire = expireTimeout ? now + expireTimeout : null;
        this._cache[idkey][subkey] = {value: value, expire: expire, refresh: now};
    }
}

// Get a value synchronously if locally available. Return `null` if no value
// is to be found (but it may exist in the network).
//
Dht.prototype.peek = function (key, subkey) {
    var keyid = (key instanceof Id) ? key : Id.fromKey(key);
    var val = this._findFromCache(keyid.toString(), subkey || keyid.toString()).value;
    if(val === undefined) return null;
    return val;
};

// Get a value synchronously if locally available. Return `null` if no value
// is to be found (but it may exist in the network).
//
Dht.prototype.peekall = function (key) {
    var keyid = (key instanceof Id) ? key : Id.fromKey(key);
    var val = this._findFromCache(keyid.toString(), undefined).value;
    if(val === undefined) return null;
    return val;
};

// Get a value from a key. Call `cb(err, value)` async. If the key/value pair
// does not exist in the system, `value` is merely `undefined` and no error is
// raised.
//
Dht.prototype.get = function (key, cb) {
    return this.multiget(key, key, cb)
}

Dht.prototype.getall = function (key, cb) {
    return this.multiget(key, null, cb)
}

Dht.prototype.multiget = function (key, subkey, cb) {
    var keyid = (key instanceof Id) ? key : Id.fromKey(key);
    var val = this.peek(keyid, subkey);
    // Force getting the key from authoritative source on getall
    if (val && subkey !== undefined)
        return process.nextTick(cb.bind(null, null, val));
    this._iterativeFindValue(keyid.toString(), subkey, cb);
};

Dht.prototype._iterativeFindValue = function (idkey, subkey, cb) {
    console.log("DHT: iterativeFindValue");
    var self  = this;
    var id    = Id.fromHex(idkey);
    var seeds = this._routes.find(id, this._opts.concurrency);
    this._routes.markRefreshed(id);
    Lookup.proceed(id, idkey, subkey, seeds, this._lookupOpts, function(err, contacts, value, expireTimeout, srcContact) {
        console.log("DHT: iterativeFindValue lookup finished");
        console.log(err);
        console.log(JSON.stringify(contacts));
        console.log(value);
        console.log(expireTimeout);
        console.log(JSON.stringify(srcContact));
        // When an iterativeFindValue succeeds, the initiator must
        // store the key/value pair at the closest node seen which did not
        // return the value. (§4.5.4)
        var closestContact = contacts[0];
        var storeValue;
        var storeSubKey = subkey;
        if(srcContact instanceof Contact) {
          if(!srcContact.id.equal(closestContact.id)) storeValue = value;
        } else {
          storeSubKey = [];
          for(var k in value) {
            if(srcContact[k].id.equal(closestContact.id)) continue;
            storeValue = storeValue || {};
            storeValue[k] = value[k];
            storeSubKey.push(k);
          }
        }
        if(storeValue) self._storeTo(idkey, storeSubKey, storeValue, closestContact, expireTimeout);
        return cb(err, value);
    });
};

// Helper provided to the Lookup algo.
//
Dht.prototype._findNodeOrValue = function (contact, targetId, idkey, subkey, cb) {
    if(typeof(cb) != 'function') throw new Error("Invalid callback");
    if(key === undefined) {
      return this._findNode(contact, targetId, cb);
    } else {
      return this._findValue(contact, targetId, idkey, subkey, cb);
    }
};

Dht.prototype._findNode = function (contact, targetId, cb) {
    var payload = {id: this._routes.id, targetId: targetId};
    var self = this;
    this.rpc.findNode(contact.endpoint, payload,
                      function onNodesFound(err, result) {
        if (err) return cb(err);
        self._discovered(contact.id, contact.endpoint);
        // If the requestor does receive a triple containing its own id, it
        // should discard it (§4.3).
        var contacts = result.contacts.filter(function(contact){
            return !contact.id.equal(self._routes.id);
        });
        return cb(null, contacts);
    });
};

Dht.prototype._findValue = function (contact, targetId, key, subkey, cb) {
    if(typeof(cb) != 'function') throw new Error("Invalid callback");
    var payload = {id: this._routes.id, targetId: targetId, idkey: idkey, subkey: subkey};
    var self = this;
    this.rpc.findValue(contact.endpoint, payload, function (err, result) {
        if(!result) return cb(err);
        return cb(err, result.contacts, result.value, result.expire);
    });
};

// Process a newly discovered contact.
//
Dht.prototype._discovered = function (id, endpoint) {
    if (!(id instanceof Id))
        throw new Error('invalid id');
    var contact = new Contact(id, endpoint);
    // FIXME @jeanlauliac We should probably not check the same 'old' contact
    // again and again. That's an opening for a DoS attack. A contact we just
    // ping-ed will probably be valid for a few minutes more, and an old
    // contact for a few hours/days more. We may want to ping the 2nd oldest,
    // 3rd, etc. but the utility is to be demonstrated.
    var oldContact = this._routes.store(contact);
    if (oldContact && !this._pendingContact) {
        var self = this;
        this._pendingContact = oldContact;
        this.rpc.ping(oldContact.endpoint, {id: this._routes.id},
                       function onPong(err, res) {
            self._pendingContact = null;
            if (!(err || !res.remoteId.equal(contact.id))) return;
            self._routes.remove(oldContact);
            self._routes.store(contact);
        });
    }
};

// Ping this DHT on the behalf of the specified `contact`.
//
Dht.prototype._onPing = function (endpoint, payload) {
    this._discovered(payload.id, endpoint);
    return {remoteId: this._routes.id};
};

// Store a key/value pair on the behalf of the specified `contact`.
//
Dht.prototype._onStore = function (endpoint, payload) {
    this._discovered(payload.id, endpoint);
    var expire = payload.expire || this._opts.expireTimeMiliseconds;
    this._storeToCache(payload.idkey, payload.subkey, payload.value, expire);
    return true;
};

// Obtain the closest known nodes from the specified `id`. Call `cb(err, ids)`.
//
Dht.prototype._onFindNode = function (endpoint, payload) {
    this._discovered(payload.id, endpoint);
    var res;
    res = this._routes.find(payload.targetId).filter(function(contact){
      // The recipient of a FIND_NODE should never return a triple
      // containing the nodeID of the requestor. (§4.3)
      return !contact.id.equal(payload.id);
    });
    return {contacts: res};
};

// Obtain the closest known nodes from the specified `id`, or return the
// value associated with `id` directly. Call `cb(err, ids)`.
//
Dht.prototype._onFindValue = function (endpoint, payload) {
    this._discovered(payload.id, endpoint);
    var val = this._findFromCache(payload.idkey, payload.subkey);
    if(val !== undefined)
        return {value: val};
    var res;
    res = this._routes.find(payload.targetId);
    return {contacts: res};
};

// Find cache value, return undefined if not found.
//
Dht.prototype._findFromCache = function (idkey, subkey) {
    this._expireCache();
    if (!this._cache.hasOwnProperty(idkey))          return {value: undefined};
    if (Object.keys(this._cache[idkey]).length == 0) return {value: undefined};
    if (subkey === undefined || subkey === null)     return cleanupValues(this._cache[idkey]);
    if (!this._cache[idkey].hasOwnProperty(subkey))  return {value: undefined};
    return this._cache[idkey][subkey];

    function cleanupValues(values) {
        var val = {}, expire = {};
        for(var k in values) {
            val[k] = values[k].value;
            expire[k] = values[k].expire;
        }
        return {value: val, expire: expire};
    }
}

module.exports = Dht;
