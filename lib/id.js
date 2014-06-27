'use strict';

var crypto = require('crypto');
var util = require('util');

// Create an id from a `buffer`.
//
var Id = function Id (buf) {
    if (buf instanceof Id) buf = buf._buf;
    if (!(buf instanceof Buffer) || buf.length !== Id.SIZE)
        throw new Error('invalid buffer');
    this._buf = buf;
};

Id.fromJSON = function(value){
    var id = Id.fromHex(value.hex);
    //console.log("id " + id + " <- " + value.hex);
    return id;
};

// Standard size for ids, in bytes. It matchs the length of a SHA-1.
//
Object.defineProperty(Id, 'SIZE', {value: 20});
Object.defineProperty(Id, 'BIT_SIZE', {value: Id.SIZE * 8});

Object.defineProperty(Id, 'SHORT_STR_PRE_LEN', {value: 5});
Object.defineProperty(Id, 'SHORT_STR_SUF_LEN', {value: 2});

Id.prototype.toJSON = function () {
    //console.log("id " + this + " -> " + this._buf.toString('hex'));
    return {
        _typename: 'Id',
        hex: this._buf.toString('hex')
    };
};

// Compute the distance between two node ids `a` and `b` expressed as Buffer.
//
Id.prototype.distanceTo = function (other) {
    if (!(other instanceof Id))
        throw new Error('can only compare to another identifier');
    var res = new Buffer(Id.SIZE);
    for (var i = 0; i < Id.SIZE; ++i) {
        res[i] = this._buf[i] ^ other._buf[i];
    }
    return res;
};

// Compare the difference of distance of two ids from this one. Return a
// Number equal to 0 if the distance is identical, >0 if `first` is closer,
// <0 otherwise.
//
Id.prototype.compareDistance = function (first, second) {
    if (!(first instanceof Id && second instanceof Id))
        throw new Error(util.format('invalid operand identifiers "%s" and "%s"',
                                    first, second));
    for (var i = 0; i < Id.SIZE; ++i) {
        var bt1 = this._buf[i] ^ first._buf[i];
        var bt2 = this._buf[i] ^ second._buf[i];
        if (bt1 > bt2) return -1;
        if (bt1 < bt2) return 1;
    }
    return 0;
};

// Test if the id is equal to another.
//
Id.prototype.equal = function (other) {
    if (!(other instanceof Id))
        throw new Error('can only compare to another identifier');
    for (var i = 0; i < Id.SIZE; ++i) {
        if (this._buf[i] != other._buf[i]) return false;
    }
    return true;
};

// Extract the bit at the specified index. The index must be between the
// range [0, id.BIT_SIZE[.
//
Id.prototype.at = function (i) {
    return (this._buf[i / 8 | 0] & (1 << (7 - i % 8))) > 0;
};

// Set the bit at the specified index. The index must be between the
// range [0, id.BIT_SIZE[.
//
Id.prototype.set = function (i, v) {
    var index = i / 8 | 0;
    var mask = 1 << (7 - i % 8);
    if (v)
        this._buf[index] |= mask;
    else
        this._buf[index] &= 255 ^ mask;
};

// Get the hex representation of the ID.
//
Id.prototype.toString = function (short) {
    var str = this._buf.toString('hex');
    if (short) {
        return util.format('%s..%s',
                           str.slice(0, Id.SHORT_STR_PRE_LEN),
                           str.slice(str.length - Id.SHORT_STR_SUF_LEN));
    }
    return str;
};

// Generate randomly a node identifer and call `cb(err, id)`.
//
Id.generate = function (cb) {
    crypto.randomBytes(Id.SIZE, function gotRndBytes(err, buf) {
        if (err) cb(err);
        cb(null, new Id(buf));
    });
};

// Create an identifier from a String `key`.
//
Id.fromKey = function (key) {
    if(key instanceof Id) {
        return key;
    }
    if(typeof key == 'string' && key.length === 40 && /^[a-f0-9]{40}$/.test(key)) {
        console.trace("Trying to convert key " + key + " to Id (already an Id)");
        return Id.fromHex(key);
    }
    var shasum = crypto.createHash('sha1');
    shasum.update(key);
    return new Id(shasum.digest());
};

// Generate randomly using Math.random() a node identifier and return it.
//
Id.generateWeak = function () {
    var b = new Buffer(Id.SIZE);
    for (var i = 0; i < Id.SIZE; ++i) {
      b[i] = Math.floor(Math.random() * (1 << 8));
    }
    return new Id(b);
};

// Create an Array prefix from a String composed of 1s and 0s.
// Eg. '101' gives [true, false, true].
//
function convertPrefix(str) {
    var res = new Array(str.length);
    for (var i = 0; i < str.length; ++i)
        res[i] = str[i] === '1';
    return res;
}

// Set the prefix of the id using the given prefix.
//
Id.prototype.setPrefix = function (prefix) {
    if (typeof prefix === 'string')
        prefix = convertPrefix(prefix);
    if (prefix.length >= Id.BIT_SIZE)
        throw new Error('id prefix is too long');
    for (var j = 0; j < prefix.length; ++j)
        this.set(j, prefix[j]);
}

// Create a predictable identifier from an Array of Boolean `prefix`. The
// prefix can also be a String of 0s and 1s.
//
Id.fromPrefix = function (prefix) {
    var id = Id.zero();
    id.setPrefix(prefix);
    return id;
};

Id.fromHex = function (prefix, suffix) {
    var id = Id.zero();
    id._buf.write(prefix, 'hex');
    if (suffix)
        id._buf.write(suffix, Id.SIZE - suffix.length / 2, 0, 'hex');
    return id;
};

// Create the zero identifier.
//
Id.zero = function () {
    var buf = new Buffer(Id.SIZE);
    for (var i = 0; i < Id.SIZE; ++i) {
        buf[i] = 0;
    }
    return new Id(buf);
};

module.exports = Id;
