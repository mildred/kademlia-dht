'use strict';

// FIXME: check the code with the 4.5 Node Lookup section of the whitepaper

var util = require('util');
var events = require('events');
var LookupList = require('./lookup-list.js');

// If subkey is undefined, perform an iterative find node. If subkey is null,
// perform an iterative find value with id as key.
//
function Lookup(id, subkey, seeds, opts) {
    if(opts === undefined) {
        opts   = seeds;
        seeds  = subkey;
        subkey = undefined;
    }
    events.EventEmitter.call(this);
    this._targetId = id;
    this._opts = opts;
    this._subkey = subkey;
    this._value = {};
    this._list = new LookupList(id, opts.size);
    this._concurrents = 0;
    // The first alpha contacts selected are used to create a shortlist for the
    // search. (§4.5)
    this._list.insertMany(seeds);
}

util.inherits(Lookup, events.EventEmitter);

// Find the closest contacts from `id` by successively emitting findNode
// requests.
//
Lookup.proceed = function (targetId, subkey, seeds, opts, cb) {
    if(cb === undefined) {
      cb = opts;
      opts = seeds;
      seeds = subkey;
      subkey = undefined;
    }
    var lookup = new Lookup(targetId, subkey, seeds, opts);
    lookup.proceed(cb);
    return lookup;
};

// Lookup algorithm (Iterative Find Node or Value) (§4.5)
//
Lookup.prototype.proceed = function (cb) {
    // The node then sends parallel, asynchronous FIND_* RPCs to the alpha
    // (concurrency) contacts in the shortlist.
    for (var i = 0; i < this._opts.concurrency; ++i) {
        var contact = this._list.next();
        if (!contact && this._concurrents === 0) {
            return cb(null, this._list.getContacts(), this._value);
        } else if (!contact) {
            break;
        }
        ++this._concurrents;
        this._forContact(contact, cb);
    }
};

// Process a single contact as part of the lookup algorithm. `state` must
// contain a `list` of the Dht.BUCKET_SIZE closest contacts known so far,
// the current `concurrency` and the final `cb` to call.
//
Lookup.prototype._forContact = function (contact, cb) {
    var self = this;
    this._opts.findNodeOrValue(contact, this._targetId, this._subkey, function (err, contacts, value) {
        if(self._abort) {
            --self._concurrents;
            return;
        }
        // Each contact, if it is live, should normally return k triples. If
        // any of the alpha contacts fails to reply, it is removed from the
        // shortlist, at least temporarily. (§4.5)
        // The node then fills the shortlist with contacts from the replies
        // received. These are those closest to the target. (§4.5)
        if (err) {
            self._list.remove(contact);
        } else {
            if (contacts) {
                self._list.insertMany(contacts);
            }
            if (value) {
                if (this._subkey !== null) {
                    cb(null, this._list.getContacts(), value, contact);
                    self._abort = true;
                } else {
                    for(k in value) self._value[k] = value[k];
                }
            }
        }
        --self._concurrents;
        if (self._concurrents === 0 && !self._abort) {
            return self.proceed(cb);
        }
    });
};

module.exports = Lookup;
