var torrentStream = require('torrent-stream');
var http = require('http');
var fs = require('fs');
var rangeParser = require('range-parser');
var url = require('url');
var mime = require('mime');
var pump = require('pump');
var path = require('path');
var proc = require('child_process');
var mkdirp = require('mkdirp');
var async = require('async');
var GrowingFile = require('growing-file');
var os = require('os');



var parseBlocklist = function(filename) {
	// TODO: support gzipped files
	var blocklistData = fs.readFileSync(filename, { encoding: 'utf8' });
	var blocklist = [];
	blocklistData.split('\n').forEach(function(line) {
		var match = null;
		if ((match = /^\s*[^#].*?\s*:\s*([a-f0-9.:]+?)\s*-\s*([a-f0-9.:]+?)\s*$/.exec(line))) {
			blocklist.push({
				start: match[1],
				end: match[2]
			});
		}
	});
	return blocklist;
};

var listRAR = function(filename, done) {
	var self = this;
	var eol = os.EOL + os.EOL;
	proc.exec('unrar vt "' + filename + '"', function (err, stdout) {
		if (err) { return done(err); }
		var chunks = stdout.split(eol);
		chunks = chunks.slice(2, chunks.length - 1);
		var list = chunks.map(function(raw){
			var desc = {};

			var props = raw.split(os.EOL);
			props.forEach(function (prop) {
				prop = prop.split(': ');
				var key = extractRARKey(prop[0]);
				var val = prop[1];
				desc[key] = val;
			});

	return desc;
		});
		done(null, list);
	})
}

function extractRARKey (key) {
  var normKey = key;
  normKey = normKey.toLowerCase();
  normKey = normKey.replace(/^\s+/, '');

  var keys = {
    'name':        'name',
    'type':        'type',
    'size':        'size',
    'packed size': 'packedSize',
    'ratio':       'ratio',
    'mtime':       'mtime',
    'attributes':  'attributes',
    'crc32':       'crc32',
    'host os':     'hostOS',
    'compression': 'compression',
    'flags':       'flags'
  };
  return keys[normKey] || key;
}


// var getLargestInDirectory = function(directory, callback) {
// 	var read = function(){
// 		var files = fs.readdirSync(directory);
// 		if (files.length == 0) {
// 			return setTimeout(read,500);
// 		}
// 		var largest = null, largestSize = 0;
// 		files.forEach(function(file){
// 			var stat = fs.statSync(path.join(directory,file));
// 			if (stat.size > largestSize) {
// 				largest = file;
// 				largestSize = stat.size;
// 			}
// 		});
// 		return callback(largest);
// 	}
// 	setTimeout(read,500);
// }
function CompositeFile(name) {
	this.length = 0;
	this.composite = true;
	this.name = name;
	this.files = [];
}
CompositeFile.prototype.addFile = function(file, index, part) {
	file.index = index;
	file.part = part;
	this.length += file.length;
	this.files.push(file);
}

CompositeFile.prototype.select = function() {
	this.files.forEach(function(file) {
		file.select();
	});
}
var parseFiles = function(filelist) {
	var file, files, largest, lg, name, partRegex, currentIdx = -1;
	partRegex = new RegExp("part\\d+")
	files = [], compositeList = {};
	filelist.forEach(function(file, i) {
		var extension, name, part, match;
		extension = file.name.substr(file.name.lastIndexOf('.') + 1);
		if (extension[0] === "r") {

			name = file.name.slice(0, -extension.length - 1);
			
			if ((match = file.name.match(partRegex)) !== null) {
				part = match[0].substr(4)
				name = name.slice(0, -(match[0].length + 1))
			} else {
				part = Number(extension.substr(1));
				if (isNaN(part)) {
					part = "00" + extension;
				} else {
					part = extension;
				}
			}
			name += ".rar";
			if (!(name in compositeList)) {
				files.push(new CompositeFile(name));
				currentIdx += 1;
				compositeList[name] = true;
			}
			files[currentIdx].addFile(file, i, part);
		} else {
			files.push(file);
			currentIdx += 1;
			files[currentIdx].index = i;
		}
	});
	files.forEach(function(file){
		if (typeof file.files !== "undefined") {
			file.files.sort(function(a,b) {
				return (a.part < b.part) ? -1 : (a.part > b.part) ? 1 : 0;
			});
		}
	});
	return files;
};
var findByName = function(name, filelist) {
	for (index in files) {
		if (files[index].name === name)
			return index;
	}
}

var extracting = {};
var downloadRAR = function(engine,archive,opts,callback) {
	if (typeof opts === "function") {
		callback = opts;
		opts = {};
	}
	// Possible race conditions?
	var extracted = path.join(engine.path, "extracted");
	// If we are on windows, then it is neccessary to copy the archive before attempting to unrar
	if (process.platform === "win32") {
		var parts = path.join(engine.path, "parts");
		mkdirp.sync(parts);
	}
	var t = archive.name;
	if (extracting[archive.name] === true) {
		var start = archive.files[0].name;
		parts = parts || path.join(engine.path, path.dirname(archive.files[0].path));
		return callback(null, { extracted: extracted, parts: parts, start: start });
	}
	extracting[archive.name] = true;
	mkdirp.sync(extracted);
	var tasks = [];
	var child = null;
	var files = archive.files;
	opts.listOnly = (typeof opts.listOnly === "undefined" || !opts.listOnly) ? false : true;
	// TODO: only read file headers for this
	if (opts.listOnly) files = files.slice(0,1); // We only need the first entry to figure out what is in the RAR
	files.forEach(function(file){
		tasks.push(function(async_callback){
			var reader = file.createReadStream();
			var parts = path.join(engine.path, path.dirname(file.path));
			if (process.platform === "win32"){
				parts = path.join(engine.path, "parts");
				reader.pipe(fs.createWriteStream(path.join(parts,file.name)))
			} else {
				reader.on('data',function(){/* noop */});
			}
			reader.on('end',function(){
				// console.log("Completed " + file.name);
				if (opts.listOnly) return callback(null,{ extracted: extracted, parts: parts, start: start });
				else if (child === null) {
					var start = files[0].name;
					// UnRAR eXtract Overwrite KeepBroken VolumePause
					// This allows unrar to wait after extracting each volume before opening the next.
					child = proc.spawn("unrar", [ "x", "-o+", "-kb", "-vp", path.join(parts,start), extracted]);
					child.stdin.setEncoding = 'utf-8';
					process.on('exit', function() { child.kill() });

					child.stdout.on('data', function (data) { console.log(data.toString()); });
					child.stderr.on('data', function (data) { console.log(data.toString()); });
					process.nextTick(function(){
						callback(null,{ extracted: extracted, parts: parts, start: start });
					});
				} else {
					child.stdin.write("C" + os.EOL); // [C]ontinue unRARing
				}
				async_callback(null);
			});
	});
});
	async.series(tasks); // Process each of them in a row, so that the output is correct
}

var pumpRange = function(request, response, file, readStream){
	var range = request.headers.range;
	range = range && rangeParser(file.length, range)[0];
	response.setHeader('Accept-Ranges', 'bytes');
	response.setHeader('Content-Type', mime.lookup(file.name));

	if (!range) {
		response.setHeader('Content-Length', file.length);
		if (request.method === 'HEAD') return response.end();
		pump(readStream(null), response);
		return;
	}

	response.statusCode = 206;
	response.setHeader('Content-Length', range.end - range.start + 1);
	response.setHeader('Content-Range', 'bytes '+range.start+'-'+range.end+'/'+file.length);

	if (request.method === 'HEAD') return response.end();
	pump(readStream(range), response);
}




var createServer = function(e, index) {
	var server = http.createServer();
	proc.spawn("unrar");
	var files = server.files = parseFiles(e.files);
	server.listRAR = function(item, callback) {
		return downloadRAR(e, item, {listOnly: true}, function(err, data){
			if(err) return callback(err);
			listRAR(path.join(data.parts, data.start),function (err, entries) {
				if(err) return callback(err);
				var list = [];
				entries.forEach(function(file, idx) {
					list.push({name:file.name, length:file.size});
				});
				return callback(null, list);
			});
		});
	}
	var onready = function() {

		if (typeof index !== 'number') {
			var largest = files.reduce(function(a, b) {
				return a.length > b.length ? a : b;
			});
			largest.select();
			server.index = largest;
			index = files.indexOf(largest);

		} else {
			files[index].select();
			server.index = files[index];
		}
	};

	if (e.torrent) onready();
	else e.on('ready', onready);

	var toJSON = function(host) {
		var list = [];
		files.forEach(function(file, i) {
			list.push({name:file.name, length:file.length, url:'http://'+host+'/'+i});
		});
		return JSON.stringify(list, null, '  ');
	};

	server.on('request', function(request, response) {
		// console.log(request);
		var u = url.parse(request.url);
		var host = request.headers.host || 'localhost';

		if (u.pathname === '/favicon.ico') 
			return response.end();
		u.pathname = u.pathname.replace(".mp4","")
		if (u.pathname === '/') {
			u.pathname = '/' + index;
		}
		if (u.pathname === '/.json') return response.end(toJSON(host));
		if (u.pathname === '/.m3u') {
			response.setHeader('Content-Type', 'application/x-mpegurl; charset=utf-8');
			return response.end('#EXTM3U\n' + files.map(function (f, i) {
				return '#EXTINF:-1,' + f.path + '\n' + 'http://'+host+'/'+i;
			}).join('\n'));
		}
	    response.setHeader('transferMode.dlna.org', 'Streaming');
	    response
	        .setHeader('contentFeatures.dlna.org',
	            'DLNA.ORG_OP=01;DLNA.ORG_CI=0;DLNA.ORG_FLAGS=017000 00000000000000000000000000');
		var params = u.pathname.slice(1).split("/");
		var i = Number(params[0]);

		if (isNaN(i) || i >= files.length) {
			response.statusCode = 404;
			response.end();
			return;
		}
		if (files[i].composite) {
			if (typeof params[1] !== "undefined" && (params[1] === ".json" || params[1] === ".m3u")) {
				return downloadRAR(e, files[i], {listOnly: true}, function(err, data){
					if (err) {
						response.statusCode = 404;
						response.end();
						return;
					}
					listRAR(path.join(data.parts, data.start),function (err, entries) {
						if (err) {
							console.log(err);
							return;
						}
						if (params[1] === ".json") {
							
							var list = [];
							entries.forEach(function(file, idx) {
								list.push({name:file.name, length:file.size, url:'http://'+host+'/'+i+"/"+idx});
							});
							return response.end(JSON.stringify(list, null, '  '));
						} else if (params[1] === ".m3u") {
							response.setHeader('Content-Type', 'application/x-mpegurl; charset=utf-8');
							return response.end('#EXTM3U\n' + entries.map(function (f, idx) {
								return '#EXTINF:-1,' + data.start + "/" + f.name + '\n' + 'http://'+host+'/'+i+"/"+idx;
							}).join('\n'));
						}
					});
				});
			}
			downloadRAR(e, files[i], function(err, data){
				if (err) {
					response.statusCode = 404;
					response.end();
					return;
				}
				listRAR(path.join(data.parts, data.start),function (err, entries) {
					if (err) {
						console.log(err);
						return;
					}
					var file;
					if (typeof params[1] !== "undefined" && params[1] !== "") {
						var idx = Number(params[1])
						if (isNaN(idx) || idx >= entries.length) {
							response.statusCode = 404;
							response.end();
							return;
						}
						file = entries[idx];
					}
					if (typeof file === "undefined")
						file = entries.reduce(function(a, b) {
							return a.size > b.size ? a : b;
						});
					var filepath = path.join(data.extracted,file.name);
					pumpRange(request, response, { length: file.size, name: file.name}, function(range) {
						if (range) {
							return GrowingFile.open(filepath, {offset: range.start, end: range.end});
						} else {
							return GrowingFile.open(filepath);
						}
					});
				});
			});

		} else {

			var file = e.files[i];
			pumpRange(request, response, file, function(range) {
				if (range) {
					return file.createReadStream(range);
				} else {
					return file.createReadStream();
				}
			});
		}
	}).on('connection', function(socket) {
		socket.setTimeout(36000000);
	});

	return server;
};

module.exports = function(torrent, opts) {
	if (!opts) opts = {};

	// Parse blocklist
	if (opts.blocklist) opts.blocklist = parseBlocklist(opts.blocklist);

	var engine = torrentStream(torrent, opts);

	// Just want torrent-stream to list files.
	if (opts.list) return engine;

	// Pause/Resume downloading as needed
	engine.on('uninterested', function() { engine.swarm.pause();  });
	engine.on('interested',   function() { engine.swarm.resume(); });

	engine.server = createServer(engine, opts.index);

	// Listen when torrent-stream is ready, by default a random port.
	engine.on('ready', function() { engine.server.listen(opts.port || 0); });

	return engine;
};
