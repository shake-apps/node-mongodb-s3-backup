'use strict';

var exec = require('child_process').exec
  , spawn = require('child_process').spawn
  , path = require('path')
  , fs = require('fs')
  , winston = require('winston');


/**
 * getArchiveName
 *
 * Returns the archive name in database_YYYY_MM_DD.tar.gz format.
 *
 * @param databaseName   The name of the database
 */
function getArchiveName(databaseName) {
  var date = new Date()
    , datestring;

  datestring = [
    databaseName,
    date.getFullYear(),
    date.getMonth() + 1,
    date.getDate(),
    date.getTime()
  ];

  return datestring.join('_') + '.tar.gz';
}

/** removeRF
 *
 * Remove a file or directory. (Recursive, forced)
 *
 * @param target       path to the file or directory
 * @param callback     callback(error)
 */
function removeRF(target, callback) {
  callback = callback || function() { };

  fs.exists(target, function(exists) {
    if (!exists) {
      return callback(null);
    }
    winston.warn("Removing " + target);
    exec( 'rm -rf ' + target, callback);
  });
}

/**
 * mongoDump
 *
 * Calls mongodump on a specified database.
 *
 * @param options    MongoDB connection options [host, port, username, password, db]
 * @param directory  Directory to dump the database to
 * @param callback   callback(err)
 */
function mongoDump(options, directory, callback) {
  var mongodump
    , mongoOptions;

  callback = callback || function() { };

  mongoOptions= [
    '-h', options.host + ':' + options.port,
    '-d', options.db,
    '-o', directory
  ];

  if(options.username && options.password) {
    mongoOptions.push('-u');
    mongoOptions.push(options.username);

    mongoOptions.push('-p');
    mongoOptions.push(options.password);
  }

  winston.info('Starting mongodump of ' + options.db);
  mongodump = spawn('mongodump', mongoOptions);

  mongodump.stdout.on('data', function (data) {
    winston.info(String(data).trim());
  });

  mongodump.stderr.on('data', function (data) {
    winston.error(String(data).trim());
  });

  mongodump.on('exit', function (code) {
    if(code === 0) {
      winston.info('mongodump executed successfully');
      callback(null);
    } else {
      callback(new Error("Mongodump exited with code " + code));
    }
  });
}

/**
 * friendlyFilesize
 *
 * Render a "friendly" human-readable version of a file size.
 *
 * @param bytes      File size in bytes
 * @returns {string}
 */
var friendlyFilesize = function(bytes) {
  var precision = 1;
  if (!bytes) {
    return '0 B';
  } else if (bytes < 1<<10) {
    return bytes + ' B';
  } else if (bytes < 1<<20) {
    return (bytes / (1<<10)).toFixed(precision) + ' KiB';
  } else if (bytes < 1<<30) {
    return (bytes / (1<<20)).toFixed(precision) + ' MiB';
  } else if (bytes < 1<<40) {
    return (bytes / (1<<30)).toFixed(precision) + ' GiB';
  } else {
    return (bytes / (1<<40)).toFixed(precision) + ' TiB';
  }
};

/**
 * compressDirectory
 *
 * Compressed the directory so we can upload it to S3.
 *
 * @param directory  current working directory
 * @param input     path to input file or directory
 * @param output     path to output archive
 * @param callback   callback(err)
 */
function compressDirectory(directory, input, output, callback) {
  var tar
    , tarOptions;

  callback = callback || function() { };

  tarOptions = [
    '-zcf',
    output,
    input
  ];

  winston.info('Starting compression of ' + input + ' into ' + output);
  tar = spawn('tar', tarOptions, { cwd: directory });

  tar.stderr.on('data', function (data) {
    winston.error(data);
  });

  tar.on('exit', function (code) {
    if(code === 0) {
      fs.stat(path.join(directory, output), function(err, stats) {
        var size = (!err) ? stats.size : 0;
        winston.info('Successfully compressed directory (' + friendlyFilesize(size) + ')');
        callback(null);
      });
    } else {
      callback(new Error("Tar exited with code " + code));
    }
  });
}

/**
 * sendToS3
 *
 * Sends a file or directory to S3.
 *
 * @param options   s3 options [key, secret, bucket]
 * @param directory directory containing file or directory to upload
 * @param target    file or directory to upload
 * @param callback  callback(err)
 */
function sendToS3(options, directory, target, callback) {
  var knox = require('knox')
    , sourceFile = path.join(directory, target)
    , s3client
    , destination = options.destination || '/';

  callback = callback || function() { };

  s3client = knox.createClient({
    key: options.key,
    secret: options.secret,
    bucket: options.bucket
  });

  winston.info('Attemping to upload ' + target + ' to the ' + options.bucket + ' s3 bucket');
  s3client.putFile(sourceFile, path.join(destination, target),  function(err, res){
    if(err) {
      return callback(err);
    }

    res.setEncoding('utf8');

    res.on('data', function(chunk){
      if(res.statusCode !== 200) {
        winston.error(chunk);
      } else {
        winston.info(chunk);
      }
    });

    res.on('end', function(chunk) {
      if (res.statusCode !== 200) {
        return callback(new Error('Expected a 200 response from S3, got ' + res.statusCode));
      }
      winston.info('Successfully uploaded to s3');
      return callback();
    });
  });
}

/**
 * sync
 *
 * Performs a mongodump on a specified database, gzips the data,
 * and uploads it to s3. Cleans up only on successful upload.
 *
 * @param mongodbConfig   mongodb config [host, port, username, password, db]
 * @param s3Config        s3 config [key, secret, bucket]
 * @param callback        callback(err)
 */
function sync(mongodbConfig, s3Config, callback) {
  var tmpDir = path.join(require('os').tmpDir(), 'mongodb_s3_backup')
    , backupDir = path.join(tmpDir, mongodbConfig.db)
    , archiveName = getArchiveName(mongodbConfig.db)
    , async = require('async');

  callback = callback || function() { };

  async.series([
    async.apply(removeRF, backupDir),
    async.apply(removeRF, path.join(tmpDir, archiveName)),
    async.apply(mongoDump, mongodbConfig, tmpDir),
    async.apply(compressDirectory, tmpDir, mongodbConfig.db, archiveName),
    async.apply(sendToS3, s3Config, tmpDir, archiveName),
    async.apply(removeRF, backupDir),
    async.apply(removeRF, path.join(tmpDir, archiveName))
  ], function(err) {
    if(err) {
      winston.error('Failed during sync: ' + err);
    } else {
      winston.info('Successfully backed up ' + mongodbConfig.db);
    }
    return callback(err);
  });
}

module.exports = { sync: sync };
