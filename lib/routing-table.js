'use strict';

var Bucket = require('./bucket.js');
var Id = require('./id.js');
var Contact = require('./contact.js');
var LookupList = require('./lookup-list.js');

// Associate Kademlia IDs with their endpoints. `localId` must be the ID of the
// local node, more close contacts are effectively stored than far contacts.
//
var RoutingTable = function (localId, bucketSize) {
    if (!(localId instanceof Id))
        throw new Error('id must be a valid identifier');
    this._bucketSize = bucketSize;
    this._endpoints  = {};
    this._root = new Bucket(bucketSize, "");
    Object.defineProperty(this, 'id', {value: localId});
};

// Force a callback to be async.
//
function makeAsync(cb) {
    return function () {
        var args = arguments;
        process.nextTick(function () {
            cb.apply(null, args);
        });
    };
}

// Refresh the routing table completely and return a Date object representing
// the time when the next refresh will have to take place.
//
RoutingTable.prototype.refresh = function (dht, refreshTimeMiliseconds) {
    var now = new Date();
    var self = this;
    var nextRefresh = now + refreshTimeMiliseconds;
    this._eachBucket(function(bucket) {
        var now = new Date();
        var bucketRefresh = bucket.nextRefreshTime(refreshTimeMiliseconds, now);
        if(!bucketRefresh) {
            bucket.refresh(dht, self);
            bucketRefresh = now + refreshTimeMiliseconds
        }
        if(nextRefresh > bucketRefresh) nextRefresh = bucketRefresh;
    });
    return nextRefresh;
};

// Mark the bucket containing `id` as refreshed (during iterativeFind*
// operations)
//
RoutingTable.prototype.markRefreshed = function (id) {
    var res = this._findBucket(id);
    res.bucket.markRefreshed();
};

// Loop over each bucket, Only cb argument is to be given, others are for
// recursion. cb is called with the bucket object and its position within the
// tree (starting at 0).
//
RoutingTable.prototype._eachBucket = function (cb, node, i) {
    i    = i    || 0;
    node = node || this._root;
    if(node instanceof Bucket) {
        cb(node, i);
        return i+1;
    } else {
        i = this._eachBucket(cb, node.left, i);
        i = this._eachBucket(cb, node.right, i);
        return i;
    }
};

// Count the number of nodes closest to the current node than `id`.
//
RoutingTable.prototype.countClosestNodes = function (id) {
    if(!(id instanceof Id))
        throw new Error('invalid id');

    var refId = this.id;
    var count = 0;
    this._eachBucket(function(bucket) {
        count += bucket.countClosestNodes(id, refId);
    });
    return count;
};

// Split a bucket, creating a new node, and insert the new contact. `this` is
// assumed to be the RoutingTable. The `opt.bucket` is split left/right
// depending on the `opt.nth` bit of the contact IDs.
//
RoutingTable.prototype._splitAndStore = function (contact, opt) {
    var node = {left:  new Bucket(this._bucketSize, opt.prefix + '0'),
                right: new Bucket(this._bucketSize, opt.prefix + '1')};
    opt.bucket.split(opt.nth, node.left, node.right);
    if (opt.parent === null)
        this._root = node;
    else if (opt.parent.left === opt.bucket)
        opt.parent.left = node;
    else
        opt.parent.right = node;
    var bucket = opt.bit ? node.right : node.left;
    return bucket.store(contact);
};

// Store (or update) a contact. Return `null` if everything went well (it does
// not always mean the contact was added, though). In case a bucket is full and
// not allowed to split, return the oldest contact of the bucket; the new
// contact is then not added. It's up to the caller to check the validity of
// the returned contact, remove it if it's invalid, and retry to add the new
// contact.
//
RoutingTable.prototype.store = function (contact) {
    if (!(contact instanceof Contact))
        throw new Error('invalid contact');

    // A node must never put its own nodeID into a bucket as a contact (§4.3).
    if (contact.id.equal(this.id)) return null;

    // Try inserting the contact in the appropriate bucket
    var res = this._findBucket(contact.id);
    if (res.bucket.store(contact)) {
        this._addRemoveEndpoint(contact.endpoint, contact.id);
        return null;
    }

    // FIXME: add the special mode splitting buckets even when we're not close.
    // The whitepaper is not very clear about it.
    if (!res.allowSplit || res.nth + 1 === Id.BIT_SIZE) {
        return res.bucket.oldest;
    }

    // Split the bucket and store the new contact
    this._splitAndStore(contact, res);
    this._addRemoveEndpoint(contact.endpoint, contact.id);
    return null;
};

// Store several contacts.
//
RoutingTable.prototype.storeSome = function (contacts) {
    for (var i = 0; i < contacts.length; ++i) {
        this.store(contacts[i]);
    }
};

// Remove the `contact`, generally because it had been detected as invalid or
// offline. Return true if a contact has been removed.
//
RoutingTable.prototype.remove = function (contact) {
    var id = (contact instanceof Contact) ? contact.id : contact;
    if (!(id instanceof Id))
        throw new Error('invalid or null id');
    var c = this._removeId(id);
    var endpoint = c ? c.endpoint : contact.endpoint;
    if(endpoint) this._addRemoveEndpoint(endpoint);
    return !!c;
};

// Remove the `contact`, generally because it had been detected as invalid or
// offline. Return that contact or false.
//
RoutingTable.prototype._removeId = function (id) {
    if (!(id instanceof Id))
        throw new Error('invalid or null id');
    var res = this._findBucket(id);
    return res.bucket.remove(id);
};

// Register an endpoint with a contact id
//
RoutingTable.prototype._addRemoveEndpoint = function (endpoint, id) {
    var endpointKey = JSON.stringify(endpoint);
    var oldId = this._endpoints[endpointKey];
    if(oldId && (!id || !oldId.equal(id))) {
        delete this._endpoints[endpointKey];
        this._removeId(oldId);
    }
    if(id) {
        this._endpoints[endpointKey] = id;
    }
};

// Find the bucket closest to the specified ID. Return an object containing
// `{parent, bucket, allowSplit, nth, bit}`
//
RoutingTable.prototype._findBucket = function (id) {
    var parent = null;
    var node = this._root;
    var allowSplit = true;
    var prefix = "";
    for (var i = 0; i < Id.BIT_SIZE; ++i) {
        var bit = id.at(i);
        allowSplit &= bit === this.id.at(i);
        if (node instanceof Bucket)
            return {parent: parent, bucket: node,
                    allowSplit: allowSplit, nth: i, bit: bit, prefix: prefix};
        parent  = node;
        node    = bit ? node.right : node.left;
        prefix += bit ? '1' : '0';
    }
};

// Find contacts in the binary tree `node`, taking `count` contacts closest to
// `id` and inserting them into `list`. `rank` is the recursion depth.
//
RoutingTable.prototype._find = function (id, rank, node, count, list) {
    if (node instanceof Bucket) {
        list.insertMany(node.obtain());
        return;
    }
    var self = this;
    function findIn(main, other) {
        self._find(id, rank + 1, main, count, list);
        if (list.length < count)
            self._find(id, rank + 1, other, count, list);
    }
    if (id.at(rank)) {
        findIn(node.right, node.left);
    } else {
        findIn(node.left, node.right);
    }
};

// Get the `count` known contacts closest from `id`.
// Ideally, it returns all contacts of the bucket closest to id. It completes
// with neighbour bucket contacts if RoutingTable.BUCKET_SIZE is not attained.
//
RoutingTable.prototype.find = function (id, count) {
    if (!(id instanceof Id))
        throw new Error('invalid id');
    if (typeof count === 'undefined') count = this._bucketSize;
    var list = new LookupList(id, count);
    this._find(id, 0, this._root, count, list);
    return list.getContacts();
};

function nodeToString(node, prefix, indent) {
    var res = '';
    if (node instanceof Bucket) {
        res += new Array(indent).join(' ') + node.toString(indent + 4) + '\n';
    } else {
        res += new Array(indent).join(' ') + '+ ' + prefix + '0:\n';
        res += nodeToString(node.left, prefix + '0', indent + 4);
        res += new Array(indent).join(' ') + '+ ' + prefix + '1:\n';
        res += nodeToString(node.right, prefix + '1', indent + 4);
    }
    return res;
}

// Get a string representation.
//
RoutingTable.prototype.toString = function (indent) {
    if (typeof indent === 'undefined') indent = 0;
    return nodeToString(this._root, '', indent);
};

module.exports = RoutingTable;
