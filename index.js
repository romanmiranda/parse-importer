'use strict';

var Minimist = require('minimist')(process.argv.slice(2));
var path = require('path');
var Parse = require('parse/node');
var SchemaController = require('parse-server/lib/Controllers/SchemaController');
var Transform = require('parse-server/lib/transform');
var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');

const importData = function(className, file, db, callback) {

  var data = require("./" + file).results;

  if (!data || !data.length) throw 'Invalid data format, please check your data file!';
  console.log("Start importing " + className);
  var index = 0;

  promiseWhile(() => {
    return index < data.length;
  }, () => {
    
    var objData = data[index++];
    
    objData = beforeTransformKeys( className, objData );
    objData = Transform.transformCreate( SchemaController.defaultColumns[className], className, objData );
    objData = afterTransformKeys( className, objData );

    var promise = new Parse.Promise();
    
    db.collection(className).insertOne( objData, function(err, result) {
      try{
        assert.equal(err, null);
        console.log("Inserted a document into the " + className + " collection.");
      }catch(e){
        console.log(e.message);
      }
      promise.resolve();
    });
    return promise;

  }).then(() => {
    callback();
  });
}

const promiseWhile = function(condition, body) {
  var promise = new Parse.Promise();

  function loop() {
    if (!condition()) return promise.resolve();
    body().then(loop, promise.reject);
  }

  loop();

  return promise;
}

function beforeTransformKeys(className, obj){
  switch(className){
    default:
      for( var el in obj){
        if( typeof obj[el].__type != 'undefined' ){
            if(  obj[el].__type == 'File'  ){
              obj[el].__type = '__TEMPFILE';
            }
        }
      }
  }
  return obj;
}

function afterTransformKeys(className, obj){
  for( var el in obj ){

    if( typeof obj[el].__type != 'undefined' ){
        if(  obj[el].__type == '__TEMPFILE'  ){
          obj[el].__type = 'File';      
        }
    }
    switch( className ){
      case '_User':
        if( el == "bcryptPassword" ){
          obj._hashed_password = obj[el];
          delete obj[el];
        }
      break;
    }
  }
  return obj;
}


try{

  var mongodbConnectionString =  Minimist.db || "mongodb://localhost:27017/parse";

  MongoClient.connect(mongodbConnectionString, function(err, db) {
    assert.equal(null, err);
    console.log("Connected correctly to MongoDB...");

    if(typeof Minimist._[0] == 'undefined'){
      console.error("Missing file to import");
      db.close();
    }else{

      var file = Minimist._[0];
      var className = path.basename(file);
      className = className.substr(0, className.lastIndexOf('.')) || className;
      importData( className , file , db, function(){
        console.log('Importing finished!');
        db.close();
        console.log("MongoDB connection closed.");
      });

    }
  });

}catch(e){
  console.log(e.message);
}
