'use strict';

var path = require('path');
var Promise = require('rsvp').Promise;
var Plugin = require('broccoli-plugin');
var mapSeries = require('promise-map-series');
var debugGenerator = require('heimdalljs-logger');
var md5Hex = require('./lib/md5-hex');
var Processor = require('./lib/processor');
var defaultProccessor = require('./lib/strategies/default');
var hashForDep = require('hash-for-dep');
var heimdall = require('heimdalljs');
var queue = require('async-promise-queue');

function ApplyPatchesSchema() {
  this.mkdir = 0;
  this.rmdir = 0;
  this.unlink = 0;
  this.change = 0;
  this.create = 0;
  this.other = 0;
  this.processed = 0;
  this.linked = 0;

  this.processString = 0;
  this.processStringTime = 0;
  this.persistentCacheHit = 0;
  this.persistentCachePrime = 0;
}

function DerivePatchesSchema() {
  this.patches = 0;
  this.entries = 0;
}

var worker = queue.async.asyncify(function(doWork) { return doWork(); });

module.exports = Filter;

Filter.prototype = Object.create(Plugin.prototype);
Filter.prototype.constructor = Filter;

function Filter(inputTree, options) {
  if (!this || !(this instanceof Filter) ||
      Object.getPrototypeOf(this) === Filter.prototype) {
    throw new TypeError('Filter is an abstract class and must be sub-classed');
  }

  var loggerName = 'broccoli-persistent-filter:' + (this.constructor.name);
  var annotation = (options && options.annotation) || this.annotation || this.description;

  if (annotation) {
    loggerName += ' > [' + annotation + ']';
  }

  this._logger = debugGenerator(loggerName);

  Plugin.call(this, [inputTree], {
    name: (options && options.name) || this.name || loggerName,
    annotation: (options && options.annotation) || this.annotation || annotation,
    fsFacade: true,
    persistentOutput: true
  });

  this.processor = new Processor(options);
  this.processor.setStrategy(defaultProccessor);

  /* Destructuring assignment in node 0.12.2 would be really handy for this! */
  if (options) {
    if (options.extensions != null)      this.extensions = options.extensions;
    if (options.targetExtension != null) this.targetExtension = options.targetExtension;
    if (options.inputEncoding != null)   this.inputEncoding = options.inputEncoding;
    if (options.outputEncoding != null)  this.outputEncoding = options.outputEncoding;
    if (options.persist) {
      this.processor.setStrategy(require('./lib/strategies/persistent'));
    }
    this.async = (options.async === true);
  }

  this.processor.init(this);

  this._canProcessCache = Object.create(null);
  this._destFilePathCache = Object.create(null);
  this._needsReset = false;

  this.concurrency = (options && options.concurrency) || Number(process.env.JOBS) || Math.max(require('os').cpus().length - 1, 1);
}

function nanosecondsSince(time) {
  var delta = process.hrtime(time);
  return delta[0] * 1e9 + delta[1];
}

function timeSince(time) {
  var deltaNS = nanosecondsSince(time);
  return (deltaNS / 1e6).toFixed(2) +' ms';
}

Filter.prototype.build = function() {
  //TODO: Should remove this when Builder is fixed.
  if (this._needsReset) {
    this.out.emptySync('');
  }

  this._needsReset = true;

  var instrumentation = heimdall.start('derivePatches', DerivePatchesSchema);
  const patches = this.in[0].changes();

  instrumentation.stats.patches = patches.length;
  instrumentation.stats.entries = this.in[0].entries.length;
  instrumentation.stop();
  var plugin = this;

  // used with options.async = true to allow 'create' and 'change' operations to complete async
  var pendingWork = [];

  return new Promise(function(resolve) {
    resolve(heimdall.node('applyPatches', ApplyPatchesSchema, function(instrumentation) {
      var prevTime = process.hrtime();
      return new Promise(function(resolve) {
        var result = mapSeries(patches, function(patch) {
          var operation = patch[0];
          var relativePath = patch[1];
          var entry = patch[2];

          plugin._logger.debug('[operation:%s] %s', operation, relativePath);

          switch (operation) {
            case 'mkdir': {
              instrumentation.mkdir++;
              return plugin.out.mkdirSync(relativePath);
            } case 'rmdir': {
              instrumentation.rmdir++;
              return plugin.out.rmdirSync(relativePath);
            } case 'unlink': {
              instrumentation.unlink++;
              return plugin.out.unlinkSync(plugin.getDestFilePath(entry) || relativePath);
            } case 'change': {
              // wrap this in a function so it doesn't actually run yet, and can be throttled
              var changeOperation = function() {
                instrumentation.change++;
                return plugin._handleFile(entry, true, instrumentation);
              };
              if (plugin.async) {
                pendingWork.push(changeOperation);
                return;
              }
              return changeOperation();
            } case 'create': {
              // wrap this in a function so it doesn't actually run yet, and can be throttled
              var createOperation = function() {
                instrumentation.create++;
                return plugin._handleFile(entry, false, instrumentation);
              };
              if (plugin.async) {
                pendingWork.push(createOperation);
                return;
              }
              return createOperation();
            } default: {
              instrumentation.other++;
            }
          }
        });
        resolve(result);
      }).then(function() {
        return queue(worker, pendingWork, plugin.concurrency);
      }).then(function(result) {
        plugin._logger.info('applyPatches', 'duration:', timeSince(prevTime), JSON.stringify(instrumentation));
        plugin._needsReset = false;
        return result;
      });
    }));
  });
};

Filter.prototype._handleFile = function(entry, isChange, instrumentation) {
  const relativePath = entry.relativePath;

  if (this.canProcessFile(relativePath, entry)) {
    instrumentation.processed++;
    return this.processAndCacheFile(this.in[0].root, this.out.root, entry, isChange, instrumentation);
  } else {
    instrumentation.linked++;
    if (isChange) {
        this.out.unlinkSync(relativePath);
    }
    return this.out.symlinkToFacadeSync(this.in[0], relativePath, relativePath);
  }
};

/*
 The cache key to be used for this plugins set of dependencies. By default
 a hash is created based on `package.json` and nested dependencies.

 Implement this to customize the cache key (for example if you need to
 account for non-NPM dependencies).

 @public
 @method cacheKey
 @returns {String}
 */
Filter.prototype.cacheKey = function() {
  return hashForDep(this.baseDir());
};

/* @public
 *
 * @method baseDir
 * @returns {String} absolute path to the root of the filter...
 */
Filter.prototype.baseDir = function() {
  throw Error('Filter must implement prototype.baseDir');
};

/**
 * @public
 *
 * optionally override this to build a more rhobust cache key
 * @param  {String} string The contents of a file that is being processed
 * @return {String}        A cache key
 */
Filter.prototype.cacheKeyProcessString = function(string, relativePath) {
  return md5Hex(string + 0x00 + relativePath);
};

// TODO: Remove `relativePath` argument (API change).
Filter.prototype.canProcessFile =
    function canProcessFile(relativePath, entry) {
  return !!this.getDestFilePath(relativePath, entry);
};

Filter.prototype.isDirectory = function(relativePath, entry) {
  return (entry || this.in[0].statSync(relativePath)).isDirectory();
};

  // TODO: Remove `relativePath` argument (API change).
Filter.prototype.getDestFilePath = function(relativePath, entry) {
  // NOTE: relativePath may have been moved or unlinked
  if (this.isDirectory(relativePath, entry)) {
    return null;
  }

  if (this.extensions == null) {
    return relativePath;
  }

  for (var i = 0, ii = this.extensions.length; i < ii; ++i) {
    var ext = this.extensions[i];
    if (relativePath.slice(-ext.length - 1) === '.' + ext) {
      if (this.targetExtension != null) {
        relativePath = relativePath.slice(0, -ext.length) + this.targetExtension;
      }
      return relativePath;
    }
  }

  return null;
};

// TODO: Remove `srcDir` and `destDir` arguments (API change).
Filter.prototype.processAndCacheFile = function(srcDir, destDir, entry, isChange, instrumentation) {
  var filter = this;
  var relativePath = entry.relativePath;

  return Promise.resolve().
      then(function asyncProcessFile() {
        return filter.processFile(srcDir, destDir, isChange, instrumentation, entry);
      }).
      then(undefined,
      // TODO(@caitp): error wrapper is for API compat, but is not particularly
      // useful.
      // istanbul ignore next
      function asyncProcessFileErrorWrapper(e) {
        if (typeof e !== 'object') e = new Error('' + e);
        e.file = relativePath;
        e.treeDir = filter.in[0].root;
        throw e;
      });
};

function invoke(context, fn, args) {
  return new Promise(function(resolve) {
    resolve(fn.apply(context, args));
  });
}

// TODO: Remove `srcDir` and `destDir` arguments (API change).
Filter.prototype.processFile = function(srcDir, destDir, isChange, instrumentation, entry) {
  var filter = this;
  var inputEncoding = this.inputEncoding;
  var outputEncoding = this.outputEncoding;
  const relativePath = entry.relativePath;

  if (inputEncoding === undefined)  inputEncoding  = 'utf8';
  if (outputEncoding === undefined) outputEncoding = 'utf8';

  var contents = this.in[0].readFileSync(relativePath, {
    encoding: inputEncoding
  });

  instrumentation.processString++;
  var processStringStart = process.hrtime();
  var string = invoke(this.processor, this.processor.processString, [this, contents, relativePath, instrumentation]);

  return string.then(function asyncOutputFilteredFile(outputString) {
    instrumentation.processStringTime += nanosecondsSince(processStringStart);
    var outputPath = filter.getDestFilePath(relativePath, entry);

    if (outputPath == null) {
      throw new Error('canProcessFile("' + relativePath +
                      '") is true, but getDestFilePath("' +
                      relativePath + '") is null');
    }

    // TODO: Is this check still necessary?  WritableTree will skip writing to
    // an existing file if the contents are the same… and do so without reading
    // from disk.
    if (isChange) {
      var isSame = this.out.readFileSync(outputPath, 'UTF-8') === outputString;
      if (isSame) {

        this._logger.debug('[change:%s] but was the same, skipping', relativePath, isSame);
        return;
      } else {
        this._logger.debug('[change:%s] but was NOT the same, writing new file', relativePath);
      }
    }

    // TODO: Is this try/catch still necessary?  WritableTree can mkdirpSync an
    // existing directory without causing I/O.
    try {
      this.out.writeFileSync(outputPath, outputString, {
        encoding: outputEncoding
      });

    } catch(e) {
      // optimistically assume the DIR was patched correctly
      this.out.mkdirpSync(path.dirname(outputPath));
      this.out.writeFileSync(outputPath, outputString, {
        encoding: outputEncoding
      });
    }

    return outputString;
  }.bind(this));
};

Filter.prototype.processString = function(/* contents, relativePath */) {
  throw new Error(
      'When subclassing broccoli-persistent-filter you must implement the ' +
      '`processString()` method.');
};

Filter.prototype.postProcess = function(result /*, relativePath */) {
  return result;
};
