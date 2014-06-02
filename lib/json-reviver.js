
var Id = require('./id.js');
var Contact = require('./contact.js');

module.exports = function(key, value){
  if(key === '') return value;
  if(typeof value === 'object') {
    if(value._typename === 'Id')      return Id.fromJSON(value);
    if(value._typename === 'Contact') return Contact.fromJSON(value);
  }
  return value;
}
