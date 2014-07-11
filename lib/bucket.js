'use strict';

var Id = require('./id.js');
var Contact = require('./contact.js');

// Store contacts ordered from oldest to latest. A contact is an object
// containing its `id` and arbitrary `endpoint` information.
//
var Bucket = function (capacity, prefix) {
    var self = this;
    this._store = [];
    if (typeof capacity !== 'number' || capacity <= 0)
        throw new Error('invalid bucket capacity');
    Object.defineProperty(this, 'prefix',   {value: prefix});
    Object.defineProperty(this, 'capacity', {value: capacity});
    Object.defineProperty(this, 'length',   {get: function(){ return self._store.length; }});
};

// Generate a pseudo-random id within that bucket range
//
Bucket.prototype.randomId = function () {
    var id = Id.generateWeak();
    id.setPrefix(this.prefix);
    return id;
};

// Perform a Kademlia refresh (§4.6)
// If no node lookups have been performed in any given bucket's range for
// tRefresh (an hour in basic Kademlia), the node selects a random number in
// that range and does a refresh, an iterativeFindNode using that number as key.
//
Bucket.prototype.refresh = function (dht, router) {
    var id = this.randomId();
    dht.iterativeFindNode(id, function(err, contacts) {
        // do nothing (?)
        // router.storeSome(contacts);
    });
};

// Mark the bucket as refreshed (during iterativeFind* operations)
//
Bucket.prototype.markRefreshed = function (now) {
    this.refreshed = now || new Date();
};

// Return false if the bucket requires a refresh (refresh time in the past).
// Else return the date of next refresh according to refreshTimeMiliseconds.
//
Bucket.prototype.nextRefreshTime = function (refreshTimeMiliseconds, now) {
    if(!this.refreshed) return false;

    var now = now || new Date();
    var refreshTime = this.refreshed + refreshTimeMiliseconds;
    return (refreshTime <= now) ? false : refreshTime;
};

Bucket.prototype.countClosestNodes = function (id, refId) {
    if (!(id instanceof Id) || !(refId instanceof Id))
        throw new Error('invalid or null id');
    var count = 0;
    for (var i = 0; i < this._store.length; ++i) {
        if(refId.compareDistance(id, this._store[i].id) > 0) count++;
    }
    return count;
};

// Store a new contact. Return `false` if there's no more space in the bucket.
// Moves the contact at the tail if it exists already.
//
Bucket.prototype.store = function (contact) {
    this.remove(contact);
    if (this._store.length == this.capacity)
        return false;
    this._store.push(contact);
    return true;
};

// Remove a contact. Return the contact that has been removed (which is a true
// value), `false` if it is nowhere to be found.
//
Bucket.prototype.remove = function (contact) {
    var id = (contact instanceof Contact) ? contact.id : contact;
    if (!(id instanceof Id))
        throw new Error('invalid or null id');
    for (var i = 0; i < this._store.length; ++i) {
        if (this._store[i].id.equal(id)) {
            return this._store.splice(i, 1)[0] || true;
        }
    }
    return false;
};

// Obtain `n` contact or less from the bucket.
//
Bucket.prototype.obtain = function (n) {
    if (typeof n === 'undefined') n = this._store.length;
    if (this._store.length <= n) return this._store;
    return this._store.slice(0, n);
};

// Split a bucket into two new buckets `left` and `right`. The split is made by
// checking the `nth` bit of each contact id. Return an object with 'left' and
// 'right' buckets.
//
Bucket.prototype.split = function (nth, left, right) {
    for (var i = 0; i < this._store.length; ++i) {
        var contact = this._store[i];
        if (contact.id.at(nth))
            right.store(contact);
        else
            left.store(contact);
    }
};

// Get a string representation.
//
Bucket.prototype.toString = function () {
    var res = '<( ';
    for (var i = 0; i < this._store.length; ++i) {
        res += this._store[i].toString(true) + ' ';
    }
    if (this.length < this.capacity)
        res += ':' + (this.capacity - this.length) + ': ';
    res += ')>';
    return res;
};

// Get the current bucket size.
//
Object.defineProperty(Bucket.prototype, 'length', {
    get: function () { return this._store.length; }
});

// Get the oldest contact of the bucket. Return `null` if the bucket is
// empty.
//
Object.defineProperty(Bucket.prototype, 'oldest', {
    get: function () {
        if (this._store.length === 0) return null;
        return this._store[0];
    }
});

module.exports = Bucket;
