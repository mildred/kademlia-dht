'use strict';

var util = require('util');
var Id = require('./id.js');

function Contact(id, endpoint) {
    if (!(id instanceof Id))
        throw new Error('the contact must have a valid id');
    this._dead = 0;
    Object.defineProperty(this, 'id', {value: id});
    Object.defineProperty(this, 'endpoint', {value: endpoint});
}

Contact.fromJSON = function (value) {
    return new Contact(value.id, value.endpoint);
}

Contact.isAlive = function (contact) {
    return contact.isAlive();
}

Contact.prototype.toJSON = function () {
    return {
        _typename: 'Contact',
        id: this.id,
        endpoint: this.endpoint
    };
};

Contact.prototype.toString = function (shortId) {
    var ids = this.id.toString(shortId);
    if (typeof this.endpoint === 'undefined')
        return ids;
    return util.format('%s/%s', ids, this.endpoint);
};

Contact.prototype.setAlive = function (alive) {
    this._dead = alive ? 0 : this._dead + 1;
};

Contact.prototype.isAlive = function (alive) {
    return this._dead == 0;
};

module.exports = Contact;
