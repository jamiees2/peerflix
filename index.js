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
var Unrar = require('unrar');

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
var getLargestInTorrent = function(filelist) {
	var file, files, largest, lg, name;
	files = {};
	filelist.forEach(function(file, i) {
		var extension, name, part;
		extension = file.name.substr(file.name.lastIndexOf('.') + 1);
		if (extension[0] === "r") {
			name = file.name.slice(0, -extension.length - 1) + ".rar";
			part = Number(extension.substr(1));
			if (isNaN(part)) {
				part = "-" + extension;
			} else {
				part = extension;
			}
			files[name] || (files[name] = {
				length: 0,
				composite: true,
				name: name
			});
			files[name][part] = file;
			files[name][part].index = i;
			files[name].length += file.length;
		} else {
			files[file.name] = file;
			files[file.name].index = i;
		}
	});
	largest = 0;
	lg = "";
	for (name in files) {
		file = files[name];
		if (file.length > largest) {
			largest = file.length;
			lg = name;
		}
	}
	return files[lg];
};
var findByName = function(name, filelist) {
	for (index in files) {
		if (files[index].name === name)
			return index;
	}
}

var extracting = {};
var downloadRAR = function(engine,files,callback) {
	// Possible race conditions?
	var extracted = path.join(engine.path, "extracted");
	var t = files.join(",");
	if (extracting[t] === true) {
		var idx = parseInt(files[0]);
		var parts = path.join(engine.path, path.dirname(engine.files[idx].path));
		var start = engine.files[idx].name;
		return callback(null, { extracted: extracted, parts: parts, start: start });
	}
	extracting[t] = true;
	mkdirp.sync(extracted);
	var tasks = [];
	var child = null;
	for(var i=0;i<files.length;i++){
		idx = parseInt(files[i]);
		if (isNaN(idx) || idx >= engine.files.length) {
			delete extracting[t];
			return callback("Non existent index!");
		}
		tasks.push(function(async_callback){
			var reader = engine.files[idx].createReadStream()
			reader.on('data',function(){/* noop */});
			reader.on('end',function(){
				// console.log("Completed " + engine.files[idx].name);

				if (child === null) {

					var start = engine.files[idx].name;
					var parts = path.join(engine.path, path.dirname(engine.files[idx].path));
					// UnRAR eXtract Overwrite KeepBroken VolumePause
					// This allows unrar to wait after extracting each volume before opening the next.
					child = proc.spawn("/usr/local/bin/unrar", [ "x", "-o+", "-kb", "-vp", path.join(parts,start), extracted]);
					child.stdin.setEncoding = 'utf-8';
					process.on('exit', function() { child.kill() });
					// child.stdout.on('data', function (data) { console.log(data.toString()); });
					// child.stderr.on('data', function (data) { console.log(data.toString()); });
					callback(null,{ extracted: extracted, parts: parts, start: start });
				} else {
					child.stdin.write("C\n"); // [C]ontinue unRARing
				}
				async_callback(null);
			});
		});
	}
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
	var onready = function() {

		if (typeof index !== 'number') {
			// index = e.files.reduce(function(a, b) {
			// 	return a.length > b.length ? a : b;
			// });
			// index = e.files.indexOf(index);
			var largest = getLargestInTorrent(e.files)
			var keys, indexes = [], files = [];
			if (largest.composite) {
				keys = Object.keys(largest);
				keys.sort();

				keys.forEach(function(id) {
					var file;
					if (id === "composite" || id === "length" || id === "name") {
						return;
					}
					file = largest[id];
					file.select();
					indexes.push(file.index);
					files.push(file);
				});
				index = indexes;
				server.index = files;
			} else {
				largest.select();
				index = largest.index;
				server.index = largest;
			}

		} else {
			e.files[index].select();
			server.index = e.files[index];
		}
	};

	if (e.torrent) onready();
	else e.on('ready', onready);

	var toJSON = function(host) {
		var list = [];
		e.files.forEach(function(file, i) {
			list.push({name:file.name, length:file.length, url:'http://'+host+'/'+i});
		});
		return JSON.stringify(list, null, '  ');
	};

	server.on('request', function(request, response) {
		var u = url.parse(request.url);
		var host = request.headers.host || 'localhost';

		if (u.pathname === '/favicon.ico') 
			return response.end();

		if (u.pathname === '/') {
			if (index instanceof Array)
				u.pathname = '/' + index.join(",");
			else
				u.pathname = '/' + index;
		}
		if (u.pathname === '/.json') return response.end(toJSON(host));
		if (u.pathname === '/.m3u') {
			response.setHeader('Content-Type', 'application/x-mpegurl; charset=utf-8');
			return response.end('#EXTM3U\n' + e.files.map(function (f, i) {
				return '#EXTINF:-1,' + f.path + '\n' + 'http://'+host+'/'+i;
			}).join('\n'));
		}

		var i = u.pathname.slice(1).split(",");
		if (i.length > 1) {
			downloadRAR(e,i, function(err, data){
				if (err) {
					response.statusCode = 404;
					response.end();
					return;
				}
				var archive = new Unrar(path.join(data.parts, data.start));
				archive.list(function (err, entries) {
					if (err) {
						console.log(err);
						return;
					}
					var file = entries.reduce(function(a, b) {
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
			i = Number(u.pathname.slice(1));

			if (isNaN(i) || i >= e.files.length) {
				response.statusCode = 404;
				response.end();
				return;
			}

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
