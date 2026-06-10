/*

TRANSMISSIONS TAPE-DECK GUI
DIRECT BUFFER LOAD / PYTHON RENDER COMPANION

FIXES
- scans supported audio files in the selected folder
- no recursive folder walking
- direct Buffer.read for every matched file
- READY only after buffers are actually added
- SCENE AUTO checks real buffer count
- can load a Python cutups live track or semi-live chunks folder
- LOAD CHUNKS auto-refreshes newly written chunk files
- LIVE TRACK follows the growing Python WAV on a dedicated layer
- SC INPUT records a rolling live-input tape buffer for SuperCollider-native live cutups
- SC CUTS can use loaded material, live input, or a blend of both
- BED LOOPS adds long drifting tape beds from material, live input, or blend

USAGE
1. Boot server
2. Run the whole block
3. Point SOURCE to a WAV folder, an audio_cutups/cutup_XX folder, or a cutup_XX_live_track.wav file
4. Press LOAD DIR, LOAD LIVE, or LOAD CHUNKS
5. Watch post window for:
   - SCAN PATTERN
   - WAV FILES FOUND
   - QUEUE LOAD
   - BUFFER OK
6. Wait for READY
7. Start layers / SCENE AUTO

*/

(
s.waitForBoot({

	var makeUI;

	// ============================================================
	// STATE
	// ============================================================

	~sampleRoot = "/Users/hgw3lls/Desktop/_TRNZ/MEDIA/auto_selected_samples/by_loop_bin";
	~loadMode = \folder;

	~buffers = List.new;
	~sampleMeta = List.new;
	~allPaths = List.new;
	~active = IdentityDictionary.new;

	~mixBus = nil;
	~master = nil;

	~liveGroup = nil;
	~liveCutGroup = nil;
	~bedGroup = nil;
	~cutsGroup = nil;
	~loopsGroup = nil;
	~ghostsGroup = nil;
	~cloudGroup = nil;

	~loaderRoutine = nil;
	~isLoading = false;
	~loadAttemptId = 0;
	~loadTargetCount = 0;
	~loadDoneCount = 0;
	~loadFailCount = 0;
	~loadedCount = 0;
	~loadedPaths = Dictionary.new;

	~chunkRefreshRoutine = nil;
	~chunkRefreshSec = 3.0;
	~chunkSourceRoot = nil;

	~liveTrackPath = nil;
	~liveTrackBuf = nil;
	~liveTrackSynth = nil;
	~liveTrackRoutine = nil;
	~liveTrackFrames = 0;
	~liveTrackAmp = 0.65;
	~liveTrackRefreshSec = 4.0;

	~liveInputBuf = nil;
	~liveInputRecorder = nil;
	~liveInputStartedAt = nil;
	~liveInputSeconds = 18.0;
	~liveInputAmp = 1.0;
	~liveInputMonitor = 0.0;
	~liveInputPreLevel = 0.86;
	~liveCutRoutine = nil;
	~liveCutAmp = 0.55;
	~scCutSourceMode = \material;

	~bedRoutine = nil;
	~bedAmp = 0.32;
	~bedMinWait = 8.0;
	~bedMaxWait = 20.0;
	~bedMinDur = 12.0;
	~bedMaxDur = 32.0;
	~bedDropout = 0.035;
	~bedReverse = false;

	~density = \medium;

	~masterAmp = 0.95;
	~driveAmt = 1.2;
	~wowAmt = 0.01;
	~flutterAmt = 0.006;
	~dropoutAmt = 0.03;
	~dirtAmt = 0.05;
	~ageAmt = 0.18;
	~dubAmt = 0.08;
	~lpFreq = 11000;
	~hpFreq = 45;

	~cutsAmp = 0.8;
	~loopsAmp = 0.75;
	~ghostsAmp = 0.55;
	~cloudAmp = 0.5;

	~cutsMuted = false;
	~loopsMuted = false;
	~ghostsMuted = false;
	~cloudMuted = false;

	~cloudVoices = 4;
	~loopVoices = 4;
	~ghostVoices = 3;

	~recordPath = Platform.userHomeDir +/+ "transmissions_tapedeck_render.aiff";

	~statusText = nil;
	~countText = nil;
	~progressBar = nil;

	~cutsBtn = nil;
	~loopsBtn = nil;
	~ghostsBtn = nil;
	~cloudBtn = nil;
	~sceneBtn = nil;
	~liveBtn = nil;
	~liveInBtn = nil;
	~liveCutBtn = nil;
	~bedBtn = nil;
	~bedReverseBtn = nil;
	~scCutSourceMenu = nil;

	// ============================================================
	// UI HELPERS
	// ============================================================

	~setStatus = { |str|
		{
			if(~statusText.notNil) {
				~statusText.string = str.asString;
			};
		}.defer;
	};

	~setCount = { |str|
		{
			if(~countText.notNil) {
				~countText.string = str.asString;
			};
		}.defer;
	};

	~setProgress = { |val|
		{
			if(~progressBar.notNil) {
				~progressBar.value = val.clip(0, 1);
			};
		}.defer;
	};

	~resetButtons = {
		{
			if(~cutsBtn.notNil) { ~cutsBtn.value = 0 };
			if(~loopsBtn.notNil) { ~loopsBtn.value = 0 };
			if(~ghostsBtn.notNil) { ~ghostsBtn.value = 0 };
			if(~cloudBtn.notNil) { ~cloudBtn.value = 0 };
			if(~sceneBtn.notNil) { ~sceneBtn.value = 0 };
			if(~liveBtn.notNil) { ~liveBtn.value = 0 };
			if(~liveInBtn.notNil) { ~liveInBtn.value = 0 };
			if(~liveCutBtn.notNil) { ~liveCutBtn.value = 0 };
			if(~bedBtn.notNil) { ~bedBtn.value = 0 };
		}.defer;
	};

	// ============================================================
	// GROUPS / CLEANUP
	// ============================================================

	~makeGroups = {
		if(~liveGroup.notNil)   { try { ~liveGroup.free } };
		if(~liveCutGroup.notNil) { try { ~liveCutGroup.free } };
		if(~bedGroup.notNil) { try { ~bedGroup.free } };
		if(~cutsGroup.notNil)   { try { ~cutsGroup.free } };
		if(~loopsGroup.notNil)  { try { ~loopsGroup.free } };
		if(~ghostsGroup.notNil) { try { ~ghostsGroup.free } };
		if(~cloudGroup.notNil)  { try { ~cloudGroup.free } };

		~liveGroup   = Group.head(s);
		~liveCutGroup = Group.after(~liveGroup);
		~bedGroup    = Group.after(~liveCutGroup);
		~cutsGroup   = Group.after(~bedGroup);
		~loopsGroup  = Group.after(~cutsGroup);
		~ghostsGroup = Group.after(~loopsGroup);
		~cloudGroup  = Group.after(~ghostsGroup);
	};

	~hardStopCuts = {
		if(~active[\cuts].notNil and: { ~active[\cuts].respondsTo(\stop) }) {
			try { ~active[\cuts].stop };
		};
		if(~cutsGroup.notNil) { try { ~cutsGroup.freeAll } };
		~active.removeAt(\cuts);
		{ if(~cutsBtn.notNil) { ~cutsBtn.value = 0 } }.defer;
	};

	~hardStopLoops = {
		if(~loopsGroup.notNil) { try { ~loopsGroup.freeAll } };
		~active.removeAt(\loops);
		{ if(~loopsBtn.notNil) { ~loopsBtn.value = 0 } }.defer;
	};

	~hardStopGhosts = {
		if(~ghostsGroup.notNil) { try { ~ghostsGroup.freeAll } };
		~active.removeAt(\ghosts);
		{ if(~ghostsBtn.notNil) { ~ghostsBtn.value = 0 } }.defer;
	};

	~hardStopCloud = {
		if(~cloudGroup.notNil) { try { ~cloudGroup.freeAll } };
		~active.removeAt(\cloud);
		{ if(~cloudBtn.notNil) { ~cloudBtn.value = 0 } }.defer;
	};

	~hardStopLiveTrack = {
		if(~liveTrackRoutine.notNil) { try { ~liveTrackRoutine.stop } };
		~liveTrackRoutine = nil;
		if(~liveTrackSynth.notNil) { try { ~liveTrackSynth.set(\gate, 0) } };
		~liveTrackSynth = nil;
		if(~liveTrackBuf.notNil) { try { ~liveTrackBuf.free } };
		~liveTrackBuf = nil;
		~liveTrackFrames = 0;
		~active.removeAt(\liveTrack);
		{ if(~liveBtn.notNil) { ~liveBtn.value = 0 } }.defer;
	};

	~hardStopLiveCutups = {
		if(~liveCutRoutine.notNil) { try { ~liveCutRoutine.stop } };
		~liveCutRoutine = nil;
		if(~liveCutGroup.notNil) { try { ~liveCutGroup.freeAll } };
		~active.removeAt(\liveCutups);
		{ if(~liveCutBtn.notNil) { ~liveCutBtn.value = 0 } }.defer;
	};

	~hardStopBedLoops = {
		if(~bedRoutine.notNil) { try { ~bedRoutine.stop } };
		~bedRoutine = nil;
		if(~bedGroup.notNil) { try { ~bedGroup.freeAll } };
		~active.removeAt(\bedLoops);
		{ if(~bedBtn.notNil) { ~bedBtn.value = 0 } }.defer;
	};

	~hardStopLiveInput = {
		if(~liveInputRecorder.notNil) { try { ~liveInputRecorder.free } };
		~liveInputRecorder = nil;
		~active.removeAt(\liveInput);
		{ if(~liveInBtn.notNil) { ~liveInBtn.value = 0 } }.defer;
	};

	~freeLiveInputBuffer = {
		~hardStopLiveCutups.();
		~hardStopLiveInput.();
		if(~liveInputBuf.notNil) { try { ~liveInputBuf.free } };
		~liveInputBuf = nil;
		~liveInputStartedAt = nil;
	};

	~stopSceneOnly = {
		if(~active[\scene].notNil and: { ~active[\scene].respondsTo(\stop) }) {
			try { ~active[\scene].stop };
		};
		~active.removeAt(\scene);
		{ if(~sceneBtn.notNil) { ~sceneBtn.value = 0 } }.defer;
	};

	~stopLayers = {
		~hardStopLiveTrack.();
		~hardStopLiveCutups.();
		~hardStopBedLoops.();
		~hardStopLiveInput.();
		~hardStopCuts.();
		~hardStopLoops.();
		~hardStopGhosts.();
		~hardStopCloud.();
	};

	~stopAll = {
		~stopSceneOnly.();
		~stopLayers.();
		~setStatus.("Stopped all");
	};

	~stopLoader = {
		~loadAttemptId = ~loadAttemptId + 1;
		~isLoading = false;
		~active.removeAt(\loader);
		~loaderRoutine = nil;
		if(~stopChunkRefresh.notNil) { ~stopChunkRefresh.() };
	};

	~freeBuffers = {
		~hardStopLiveTrack.();
		~hardStopBedLoops.();
		~freeLiveInputBuffer.();
		~buffers.do { |b| try { b.free } };
		~buffers.clear;
		~sampleMeta.clear;
		~allPaths.clear;
		~loadedPaths.clear;
		~loadDoneCount = 0;
		~loadFailCount = 0;
		~loadedCount = 0;
		~loadTargetCount = 0;
		~setCount.("Loaded: 0");
		~setProgress.(0);
	};

	// Single source of truth for usable loaded audio.
	~buffersReady = {
		^(~loadedCount > 0)
		and: { ~buffers.notEmpty }
		and: { ~sampleMeta.notEmpty }
		and: { ~buffers.size == ~sampleMeta.size };
	};

	~loadingInProgress = {
		^(~isLoading)
		and: { ~loadTargetCount > 0 }
		and: { ~loadDoneCount < ~loadTargetCount };
	};

	~canStartPlayback = { |btn=nil|
		if(~loadingInProgress.()) {
			~setStatus.("Loading in progress — wait for READY");
			{ if(btn.notNil) { btn.value = 0 } }.defer;
			^false;
		};

		if(~buffersReady.().not) {
			~setStatus.("No buffers loaded — press LOAD DIR/LIVE/CHUNKS first");
			{ if(btn.notNil) { btn.value = 0 } }.defer;
			^false;
		};

		^true;
	};

	// ============================================================
	// AUDIO / PYTHON RENDER SCAN
	// ============================================================

	~isSupportedAudioPath = { |path|
		var lower;

		lower = path.asString.toLower;
		^((lower.endsWith(".wav")) or: {
			(lower.endsWith(".aif")) or: {
				(lower.endsWith(".aiff")) or: {
					lower.endsWith(".flac")
				}
			}
		});
	};

	~scanWavFolder = { |root|
		var files, normalizedRoot, pattern, matches;

		normalizedRoot = root.asString.standardizePath;

		if(File.exists(normalizedRoot).not) {
			("Path not found: " ++ normalizedRoot).warn;
			~setStatus.("Path not found");
			~setCount.("Loaded: 0");
			~setProgress.(0);
			^List.new;
		};

		if(PathName.new(normalizedRoot).isFile) {
			if(~isSupportedAudioPath.(normalizedRoot)) {
				~setStatus.("Single audio file selected");
				~setCount.("Found: 1");
				~setProgress.(0.2);
				("AUDIO FILE SELECTED: " ++ normalizedRoot).postln;
				^[normalizedRoot].as(List);
			} {
				("Unsupported audio file: " ++ normalizedRoot).warn;
				~setStatus.("Use wav/aif/aiff/flac");
				~setCount.("Loaded: 0");
				~setProgress.(0);
				^List.new;
			};
		};

		~setStatus.("Scanning audio folder...");
		~setCount.("Looking for audio files...");
		~setProgress.(0.1);

		files = List.new;
		["wav", "aif", "aiff", "flac"].do { |ext|
			pattern = normalizedRoot +/+ ("*." ++ ext);
			("SCAN PATTERN: " ++ pattern).postln;
			matches = pattern.pathMatch;
			if(matches.notNil) {
				matches.do { |path| files.add(path) };
			};
		};
		files = files.asArray.sort;

		("AUDIO FILES FOUND: " ++ files.size).postln;

		if(files.isEmpty) {
			~setStatus.("No supported audio files found");
			~setCount.("Loaded: 0");
			~setProgress.(0);
			^List.new;
		};

		~setStatus.("Scan complete");
		~setCount.("Found: " ++ files.size);
		~setProgress.(0.2);

		^files.as(List);
	};

	~variantDirFromPath = { |root|
		var path;

		path = root.asString.standardizePath;

		if(File.exists(path).not) {
			^path;
		};

		if(PathName.new(path).isFile) {
			^PathName.new(path).pathOnly;
		};

		^path;
	};

	~scanLiveTrack = { |root|
		var path, dir, files, pattern;

		path = root.asString.standardizePath;

		if(File.exists(path) and: { PathName.new(path).isFile }) {
			~setStatus.("Live track selected");
			~setCount.("Found: 1");
			~setProgress.(0.2);
			("LIVE TRACK: " ++ path).postln;
			^[path].as(List);
		};

		dir = ~variantDirFromPath.(path);
		pattern = dir +/+ "*_live_track.wav";
		("LIVE TRACK PATTERN: " ++ pattern).postln;
		files = pattern.pathMatch;
		if(files.isNil) { files = [] };
		files = files.asArray.sort;

		if(files.isEmpty) {
			~setStatus.("No *_live_track.wav found");
			~setCount.("Loaded: 0");
			~setProgress.(0);
			^List.new;
		};

		~setStatus.("Live track found");
		~setCount.("Found: 1");
		~setProgress.(0.2);
		^[files.last].as(List);
	};

	~scanChunksFolder = { |root, quiet=false|
		var path, dir, chunksDir, files, pattern;

		path = root.asString.standardizePath;

		if(File.exists(path).not) {
			if(quiet.not) {
				~setStatus.("Path not found");
				~setCount.("Loaded: 0");
				~setProgress.(0);
			};
			^List.new;
		};

		if(PathName.new(path).isFile) {
			dir = PathName.new(path).pathOnly;
		} {
			dir = path;
		};

		chunksDir = dir;
		pattern = chunksDir +/+ "*.wav";
		if(quiet.not) { ("CHUNKS PATTERN: " ++ pattern).postln };
		files = pattern.pathMatch;
		if(files.isNil) { files = [] };
		files = files.asArray.sort;

		if(files.isEmpty) {
			chunksDir = dir +/+ "chunks";
			pattern = chunksDir +/+ "*.wav";
			if(quiet.not) { ("CHUNKS PATTERN: " ++ pattern).postln };
			files = pattern.pathMatch;
			if(files.isNil) { files = [] };
			files = files.asArray.sort;
		};

		if(files.isEmpty) {
			if(quiet.not) {
				~setStatus.("No chunk .wav files found");
				~setCount.("Loaded: 0");
				~setProgress.(0);
			};
			^List.new;
		};

		if(quiet.not) {
			~setStatus.("Chunks found");
			~setCount.("Found: " ++ files.size);
			~setProgress.(0.2);
		};
		^files.as(List);
	};

	~scanCurrentSource = {
		var root;

		root = ~sampleRoot.asString.standardizePath;

		^switch(
			~loadMode,
			\live, { ~scanLiveTrack.(root) },
			\chunks, { ~scanChunksFolder.(root) },
			{ ~scanWavFolder.(root) }
		);
	};

	// ============================================================
	// CHOOSERS
	// ============================================================

	~chooseIndex = { |mode = \any|
		var idxs, candidates;

		if(~buffers.isEmpty) { ^nil };

		idxs = (0 .. (~buffers.size - 1));

		candidates = switch(
			mode,
			\micro,  { idxs.select { |i| ~sampleMeta[i][\dur] <= 1.5 } },
			\short,  { idxs.select { |i| (~sampleMeta[i][\dur] > 1.5) and: { ~sampleMeta[i][\dur] <= 5.0 } } },
			\phrase, { idxs.select { |i| (~sampleMeta[i][\dur] > 5.0) and: { ~sampleMeta[i][\dur] <= 12.0 } } },
			\long,   { idxs.select { |i| ~sampleMeta[i][\dur] > 12.0 } },
			\any,    { idxs },
			{ idxs }
		);

		if(candidates.isEmpty) { candidates = idxs };
		^candidates.choose;
	};

	~weightedChooseIndex = { |shortBias = 0.8|
		var idxs, weights;

		if(~buffers.isEmpty) { ^nil };

		idxs = (0 .. (~buffers.size - 1));

		weights = idxs.collect { |i|
			var d, w;
			d = ~sampleMeta[i][\dur];
			w = 1.0;

			if(d <= 1.0) { w = w + (shortBias * 5.0) };
			if((d > 1.0) and: { d <= 3.0 }) { w = w + (shortBias * 3.5) };
			if((d > 3.0) and: { d <= 7.0 }) { w = w + 2.0 };
			if(d > 7.0) { w = w + 0.6 };

			w.max(0.01);
		};

		^idxs.wchoose(weights.normalizeSum);
	};

	// ============================================================
	// LOADING
	// ============================================================

	~appendChunkPath = { |path|
		var normalized;

		normalized = path.asString.standardizePath;

		if(~loadedPaths[normalized].notNil) {
			^nil;
		};

		~loadedPaths[normalized] = \queued;

		Buffer.read(s, normalized, action: { |b|
			var dur;

			if(b.notNil and: { b.numFrames > 0 }) {
				dur = b.numFrames / b.sampleRate;

				~buffers.add(b);
				~sampleMeta.add((
					path: normalized,
					name: PathName.new(normalized).fileName,
					dur: dur,
					frames: b.numFrames,
					sr: b.sampleRate,
					bufnum: b.bufnum
				));
				~allPaths.add(normalized);
				~loadedPaths[normalized] = true;
				~loadedCount = ~loadedCount + 1;
				~loadTargetCount = ~loadTargetCount.max(~loadedCount);

				("CHUNK BUFFER OK: " ++ PathName.new(normalized).fileName ++ " | loaded=" ++ ~loadedCount).postln;
				~setCount.("Loaded: " ++ ~loadedCount ++ " / " ++ ~loadTargetCount);
				~setStatus.("Chunk loaded: " ++ PathName.new(normalized).fileName);
				if(~loadTargetCount > 0) {
					~setProgress.(~loadedCount / ~loadTargetCount);
				};
			} {
				~loadedPaths.removeAt(normalized);
				if(b.notNil) { try { b.free } };
				("CHUNK BUFFER FAIL: " ++ normalized).warn;
			};
		});
	};

	~refreshChunksOnce = {
		var root, files, newFiles;

		root = if(~chunkSourceRoot.notNil) {
			~chunkSourceRoot.asString.standardizePath;
		} {
			~sampleRoot.asString.standardizePath;
		};

		files = ~scanChunksFolder.(root, true);
		newFiles = files.select { |path| ~loadedPaths[path.asString.standardizePath].isNil };

		if(newFiles.notEmpty) {
			~loadTargetCount = ~loadTargetCount.max(~loadedCount + newFiles.size);
			("CHUNK REFRESH: new=" ++ newFiles.size ++ " root=" ++ root).postln;
			newFiles.do { |path| ~appendChunkPath.(path) };
		};
	};

	~stopChunkRefresh = {
		if(~chunkRefreshRoutine.notNil) { try { ~chunkRefreshRoutine.stop } };
		~chunkRefreshRoutine = nil;
		~active.removeAt(\chunkRefresh);
	};

	~startChunkRefresh = {
		~stopChunkRefresh.();
		~chunkSourceRoot = ~sampleRoot.asString.standardizePath;
		~chunkRefreshRoutine = Routine({
			loop {
				~chunkRefreshSec.wait;
				~refreshChunksOnce.();
			};
		}).play(SystemClock);
		~active[\chunkRefresh] = ~chunkRefreshRoutine;
		~setStatus.("Chunk auto-refresh on");
	};

	~loadOnePath = { |path, attemptId|
		Buffer.read(s, path, action: { |b|
			var dur, normalized;

			normalized = path.asString.standardizePath;

			if(attemptId != ~loadAttemptId) {
				if(b.notNil) { try { b.free } };
				^nil;
			};

			~loadDoneCount = ~loadDoneCount + 1;

			if(b.notNil and: { b.numFrames > 0 }) {
				dur = b.numFrames / b.sampleRate;

				~buffers.add(b);
				~sampleMeta.add((
					path: path,
					name: PathName.new(path).fileName,
					dur: dur,
					frames: b.numFrames,
					sr: b.sampleRate,
					bufnum: b.bufnum
				));
				~loadedPaths[normalized] = true;

				~loadedCount = ~loadedCount + 1;

				("BUFFER OK: " ++ PathName.new(path).fileName ++ " | loaded=" ++ ~loadedCount ++ " done=" ++ ~loadDoneCount ++ "/" ++ ~loadTargetCount).postln;

				~setCount.("Loaded: " ++ ~loadedCount ++ " / " ++ ~loadTargetCount);
				~setStatus.("Loaded: " ++ PathName.new(path).fileName);

				if(~loadTargetCount > 0) {
					~setProgress.(0.25 + ((~loadedCount / ~loadTargetCount) * 0.75));
				};

			} {
				~loadFailCount = ~loadFailCount + 1;
				("FAILED TO LOAD BUFFER: " ++ path).warn;
				("BUFFER FAIL: " ++ PathName.new(path).fileName ++ " | fail=" ++ ~loadFailCount ++ " done=" ++ ~loadDoneCount ++ "/" ++ ~loadTargetCount).postln;
				if(b.notNil) { try { b.free } };
			};

			if(~loadDoneCount >= ~loadTargetCount) {
				~isLoading = false;
				~active.removeAt(\loader);
				~loaderRoutine = nil;

				if(~buffersReady.()) {
					("READY: buffers=" ++ ~buffers.size ++ " sampleMeta=" ++ ~sampleMeta.size ++ " fail=" ++ ~loadFailCount).postln;
					~setStatus.("READY");
					~setCount.("Loaded: " ++ ~loadedCount ++ " / " ++ ~loadTargetCount);
					~setProgress.(1.0);
					if(~loadMode == \chunks) {
						~startChunkRefresh.();
					};
					if(~loadMode == \live) {
						if(~allPaths.notEmpty) {
							~startLiveTrack.(~allPaths[0]);
						};
					};
				} {
					("READY FAILED: loaded=" ++ ~loadedCount ++ " target=" ++ ~loadTargetCount ++ " fail=" ++ ~loadFailCount).warn;
					~setStatus.("Load failed — no usable buffers");
					~setCount.("Loaded: " ++ ~loadedCount ++ " / " ++ ~loadTargetCount ++ " (fail: " ++ ~loadFailCount ++ ")");
					~setProgress.(0);
				};
			};
		});
	};

	~loadSamplesAsync = {
		var files;

		var attemptId;

		~sampleRoot = ~sampleRoot.asString.standardizePath;
		("LOAD -> " ++ ~sampleRoot ++ " mode=" ++ ~loadMode.asString).postln;

		~stopLoader.();
		~stopAll.();
		~freeBuffers.();

		files = ~scanCurrentSource.();

		if(files.isEmpty) {
			~setStatus.("No usable wav files found");
			~setCount.("Loaded: 0");
			~setProgress.(0);
			^nil;
		};

		~allPaths = files;
		~loadTargetCount = ~allPaths.size;
		~loadDoneCount = 0;
		~loadFailCount = 0;
		~loadedCount = 0;
		~isLoading = true;
		~loadAttemptId = ~loadAttemptId + 1;
		attemptId = ~loadAttemptId;

		~setStatus.("Loading wav files...");
		~setCount.("Loaded: 0 / " ++ ~loadTargetCount);
		~setProgress.(0.25);

		~allPaths.do { |path|
			("QUEUE LOAD: " ++ path).postln;
			~loadOnePath.(path, attemptId);
		};
	};

	// ============================================================
	// SYNTHDEFS
	// ============================================================

	SynthDef(\cutFragTape, {
		|out=0, buf=0, amp=0.3, rate=1.0, start=0.0, pan=0.0, atk=0.003, rel=0.08, lp=12000, hp=70, revMix=0.12|
		var frames, pos, env, sig, wet;

		frames = BufFrames.kr(buf);
		pos = start * frames;
		env = EnvGen.kr(Env.perc(atk, rel, 1, -4), doneAction: 2);
		sig = PlayBuf.ar(1, buf, BufRateScale.kr(buf) * rate, startPos: pos, doneAction: 0);
		sig = HPF.ar(sig, hp);
		sig = LPF.ar(sig, lp);
		sig = sig * env * amp;
		sig = Pan2.ar(sig, pan);
		wet = FreeVerb2.ar(sig[0], sig[1], mix: 0.22, room: 0.8, damp: 0.35);
		sig = XFade2.ar(sig, wet, (revMix * 2) - 1);
		Out.ar(out, sig);
	}).add;

	SynthDef(\concreteCutTape, {
		|out=0, buf=0, amp=0.28, rate=1.0, start=0.0, dur=0.5, pan=0.0, fx=0.55, lp=9000, hp=90|
		var frames, pos, env, raw, sweep, comb, dly, ring, pitch, phase, clip, shaped, sig;

		frames = BufFrames.kr(buf);
		pos = start * frames;
		env = EnvGen.kr(Env.linen(0.004, dur.max(0.02), 0.03, 1, -4), doneAction: 2);
		raw = PlayBuf.ar(1, buf, BufRateScale.kr(buf) * rate, startPos: pos, doneAction: 0);
		sweep = XLine.kr(lp.max(400), hp.max(80), dur.max(0.03)).clip(120, 12000);
		raw = RLPF.ar(HPF.ar(raw, hp), sweep, 0.28);
		comb = CombC.ar(raw, 0.45, LFNoise1.kr(0.6).range(0.035, 0.22), 1.8);
		dly = DelayC.ar(raw, 0.35, LFNoise1.kr(0.4).range(0.04, 0.18));
		ring = raw * SinOsc.ar(LFNoise1.kr(0.25).range(90, 900));
		pitch = PitchShift.ar(raw + WhiteNoise.ar(0.01), 0.08, LFNoise1.kr(0.08).range(0.78, 1.24), 0.01, 0.04);
		phase = RLPF.ar(raw, SinOsc.kr(0.12).range(320, 2400), 0.18);
		clip = Clip.ar(raw * LFNoise1.kr(0.5).range(1.4, 4.0), -0.6, 0.6);
		shaped = (raw * 0.30) + (comb * 0.16) + (dly * 0.12) + (ring * 0.11) + (pitch * 0.13) + (phase * 0.10) + (clip * 0.18);
		sig = XFade2.ar(raw, shaped, (fx.clip(0, 1) * 2) - 1);
		sig = Pan2.ar(sig * env * amp, pan);
		sig = FreeVerb2.ar(sig[0], sig[1], mix: 0.18, room: 0.75, damp: 0.38);
		Out.ar(out, sig);
	}).add;

	SynthDef(\loopCellTape, {
		|out=0, buf=0, amp=0.22, rate=1.0, start=0.0, end=0.2, pan=0.0, atk=0.01, rel=0.2, lp=8500, hp=120, trigRate=4|
		var frames, startPos, endPos, trig, phase, sig, env;

		frames = BufFrames.kr(buf);
		startPos = start * frames;
		endPos = max(startPos + 2, end * frames);
		trig = Impulse.kr(trigRate);
		phase = Phasor.ar(trig, BufRateScale.kr(buf) * rate, startPos, endPos, startPos);
		sig = BufRd.ar(1, buf, phase, loop: 1);
		env = EnvGen.kr(Env.asr(atk, 1, rel), doneAction: 2);
		sig = HPF.ar(sig, hp);
		sig = LPF.ar(sig, lp);
		sig = Pan2.ar(sig * amp * env, pan);
		Out.ar(out, sig);
	}).add;

	SynthDef(\liveTrackTape, {
		|out=0, buf=0, amp=0.65, rate=1.0, gate=1, atk=0.08, rel=1.2, lp=16000, hp=30|
		var env, sig;

		env = EnvGen.kr(Env.asr(atk, 1, rel), gate, doneAction: 2);
		sig = PlayBuf.ar(2, buf, BufRateScale.kr(buf) * rate, loop: 1);
		sig = HPF.ar(LPF.ar(sig, lp), hp);
		Out.ar(out, sig * env * amp);
	}).add;

	SynthDef(\liveInputRecorderTape, {
		|out=0, buf=0, inA=0, inB=1, amp=1.0, monitor=0.0, recLevel=1.0, preLevel=0.86|
		var sig;

		sig = SoundIn.ar([inA, inB]) * amp;
		sig = LeakDC.ar(sig);
		RecordBuf.ar(sig, buf, recLevel: recLevel, preLevel: preLevel, loop: 1);
		Out.ar(out, sig * monitor);
	}).add;

	SynthDef(\liveInputCutTape, {
		|out=0, buf=0, amp=0.28, rate=1.0, start=0.0, dur=0.24, pan=0.0, atk=0.004, rel=0.06, lp=9500, hp=80, revMix=0.10|
		var frames, pos, env, sig, wet;

		frames = BufFrames.kr(buf);
		pos = start.clip(0, 0.999) * frames;
		env = EnvGen.kr(Env.linen(atk, dur.max(0.02), rel, 1, -4), doneAction: 2);
		sig = PlayBuf.ar(2, buf, BufRateScale.kr(buf) * rate, startPos: pos, doneAction: 0);
		sig = LPF.ar(HPF.ar(sig, hp), lp);
		sig = Balance2.ar(sig[0], sig[1], pan, amp) * env;
		wet = FreeVerb2.ar(sig[0], sig[1], mix: 0.28, room: 0.84, damp: 0.32);
		sig = XFade2.ar(sig, wet, (revMix.clip(0, 1) * 2) - 1);
		Out.ar(out, sig);
	}).add;

	SynthDef(\bedLoopTape, {
		|out=0, buf=0, amp=0.26, rate=1.0, loopStart=0.0, loopEnd=1.0, playDur=20.0, pan=0.0, reverse=0, dropout=0.03, wow=0.012, flutter=0.004, lp=9000, hp=55|
		var frames, startPos, endPos, resetPos, dir, wobble, phase, env, sig, flg, noise, gate;

		frames = BufFrames.kr(buf);
		startPos = loopStart.clip(0, 0.998) * frames;
		endPos = max(startPos + 2, loopEnd.clip(0.001, 1.0) * frames);
		resetPos = Select.kr(reverse > 0.5, [startPos, endPos - 2]);
		dir = Select.kr(reverse > 0.5, [1, -1]);
		wobble = SinOsc.kr(0.17, 0, wow, 1) * SinOsc.kr(4.3, 0, flutter, 1) * LFNoise1.kr(0.05).range(0.992, 1.008);
		phase = Phasor.ar(0, BufRateScale.kr(buf) * rate * dir * wobble, startPos, endPos, resetPos);
		env = EnvGen.kr(Env.linen(0.35, playDur.max(0.5), 2.2, 1, -4), doneAction: 2);
		sig = BufRd.ar(1, buf, phase, loop: 1, interpolation: 4);
		flg = CombL.ar(sig, 0.035, SinOsc.kr(0.18).range(0.004, 0.024), 2.5);
		noise = WhiteNoise.ar(0.006);
		gate = Lag.kr((LFNoise1.kr(5.0) > (-1 + (dropout.clip(0, 1) * 2))).max(0), 0.05);
		sig = ((sig * 0.82) + (flg * 0.12) + noise).tanh;
		sig = HPF.ar(LPF.ar(sig, lp), hp) * env * gate;
		Out.ar(out, Pan2.ar(sig * amp, pan));
	}).add;

	SynthDef(\liveBedLoopTape, {
		|out=0, buf=0, amp=0.26, rate=1.0, loopStart=0.0, loopEnd=1.0, playDur=20.0, pan=0.0, reverse=0, dropout=0.03, wow=0.012, flutter=0.004, lp=9000, hp=55|
		var frames, startPos, endPos, resetPos, dir, wobble, phase, env, sig, flg, noise, gate;

		frames = BufFrames.kr(buf);
		startPos = loopStart.clip(0, 0.998) * frames;
		endPos = max(startPos + 2, loopEnd.clip(0.001, 1.0) * frames);
		resetPos = Select.kr(reverse > 0.5, [startPos, endPos - 2]);
		dir = Select.kr(reverse > 0.5, [1, -1]);
		wobble = SinOsc.kr(0.14, 0, wow, 1) * SinOsc.kr(3.7, 0, flutter, 1) * LFNoise1.kr(0.04).range(0.992, 1.008);
		phase = Phasor.ar(0, BufRateScale.kr(buf) * rate * dir * wobble, startPos, endPos, resetPos);
		env = EnvGen.kr(Env.linen(0.35, playDur.max(0.5), 2.2, 1, -4), doneAction: 2);
		sig = BufRd.ar(2, buf, phase, loop: 1, interpolation: 4);
		flg = CombL.ar(sig, 0.035, SinOsc.kr(0.16).range(0.004, 0.022), 2.2);
		noise = WhiteNoise.ar(0.005 ! 2);
		gate = Lag.kr((LFNoise1.kr(4.5) > (-1 + (dropout.clip(0, 1) * 2))).max(0), 0.05);
		sig = ((sig * 0.82) + (flg * 0.12) + noise).tanh;
		sig = HPF.ar(LPF.ar(sig, lp), hp) * env * gate;
		Out.ar(out, Balance2.ar(sig[0], sig[1], pan, amp));
	}).add;

	SynthDef(\ghostPlayerTape, {
		|out=0, buf=0, amp=0.14, rate=(-0.5), start=0.85, pan=0, lp=3500, hp=220, rel=3.5|
		var frames, pos, env, sig;

		frames = BufFrames.kr(buf);
		pos = start * frames;
		env = EnvGen.kr(Env.linen(0.01, rel, 0.5, 1, -4), doneAction: 2);
		sig = PlayBuf.ar(1, buf, BufRateScale.kr(buf) * rate, startPos: pos, doneAction: 0);
		sig = HPF.ar(sig, hp);
		sig = LPF.ar(sig, lp);
		sig = sig * env * amp;
		sig = Pan2.ar(sig, pan);
		sig = FreeVerb2.ar(sig[0], sig[1], mix: 0.4, room: 0.92, damp: 0.25);
		Out.ar(out, sig);
	}).add;

	SynthDef(\grainBufCloudTape, {
		|out=0, buf=0, amp=0.12, dens=16, posRate=0.12, rate=1.0, gdur=0.08, panWidth=0.8, lp=9000, hp=110|
		var trig, pos, pan, sig;

		trig = Dust.kr(dens);
		pos = LFNoise1.kr(posRate).range(0.0, 1.0);
		pan = LFNoise1.kr(1.0).range(panWidth.neg, panWidth);
		sig = TGrains.ar(2, trig, buf, rate, pos * BufDur.kr(buf), gdur, pan, amp);
		sig = HPF.ar(sig, hp);
		sig = LPF.ar(sig, lp);
		Out.ar(out, sig);
	}).add;

	SynthDef(\tapeDeckMaster, {
		|in=0, out=0, amp=1.0, drive=1.2, wow=0.01, flutter=0.006, dropout=0.03, dirt=0.05, age=0.18, dub=0.08, lp=11000, hp=45|
		var dry, sig, wowMod, flutterMod, hiss, crackle, rumble, hum, gate, delA, delB;

		dry = In.ar(in, 2);
		wowMod = SinOsc.kr(0.18, 0, wow, 1.0);
		flutterMod = SinOsc.kr(5.8, 0, flutter, 1.0);
		sig = DelayC.ar(dry, 0.05, (((wowMod * flutterMod) - 1).abs * 0.01));
		sig = tanh(sig * drive);

		hiss = HPF.ar(WhiteNoise.ar(dirt * 0.05), 5000);
		crackle = Decay2.ar(Dust.ar((dirt * 100).max(1)), 0.0008, 0.01) * WhiteNoise.ar(dirt * 0.12);
		rumble = LPF.ar(BrownNoise.ar((dirt + age) * 0.025), 95);
		hum = SinOsc.ar([60, 120], 0, dirt * age * 0.025);

		delA = DelayC.ar(sig, 0.3, 0.11);
		delB = DelayC.ar(sig, 0.4, 0.22);

		gate = Lag.kr((LFNoise1.kr(4.5) > (-1 + (dropout * 2))).max(0), 0.02);

		sig = sig + (delA * dub) + (delB * (dub * 0.7));
		sig = sig + hiss + rumble + hum + Pan2.ar(crackle, LFNoise1.kr(1.2));
		sig = sig * gate;
		sig = HPF.ar(sig, hp);
		sig = LPF.ar(sig, (lp * (1 - (age * 0.35))).clip(600, 18000));
		sig = BPeakEQ.ar(sig, (850 + (age * 1900)).clip(200, 4000), 0.7, age * -4);
		sig = LeakDC.ar(sig);
		sig = CompanderD.ar(sig, 0.6, 1, 0.7, 0.01, 0.14);

		Out.ar(out, sig * amp);
	}).add;

	// ============================================================
	// AUDIO STRUCTURE
	// ============================================================

	s.bind {
		~mixBus = Bus.audio(s, 2);
		~makeGroups.();

		if(~master.notNil) { try { ~master.free } };

		~master = Synth.tail(s, \tapeDeckMaster, [
			\in, ~mixBus,
			\out, 0,
			\amp, ~masterAmp,
			\drive, ~driveAmt,
			\wow, ~wowAmt,
			\flutter, ~flutterAmt,
			\dropout, ~dropoutAmt,
			\dirt, ~dirtAmt,
			\age, ~ageAmt,
			\dub, ~dubAmt,
			\lp, ~lpFreq,
			\hp, ~hpFreq
		]);
	};

	~updateMasterFX = {
		if(~master.notNil) {
			~master.set(
				\amp, ~masterAmp,
				\drive, ~driveAmt,
				\wow, ~wowAmt,
				\flutter, ~flutterAmt,
				\dropout, ~dropoutAmt,
				\dirt, ~dirtAmt,
				\age, ~ageAmt,
				\dub, ~dubAmt,
				\lp, ~lpFreq,
				\hp, ~hpFreq
			);
		};
	};

	~updateLiveTrackAmp = {
		if(~liveTrackSynth.notNil) {
			~liveTrackSynth.set(\amp, ~liveTrackAmp);
		};
	};

	~resolveLiveTrackPath = { |root|
		var path, files;

		path = root.asString.standardizePath;

		if(File.exists(path) and: { PathName.new(path).isFile }) {
			^path;
		};

		files = ~scanLiveTrack.(path);
		if(files.notEmpty) {
			^files[0].asString.standardizePath;
		};

		^nil;
	};

	~readLiveTrackBuffer = { |force=false|
		var path;

		path = ~liveTrackPath;

		if(path.isNil or: { File.exists(path).not }) {
			~setStatus.("Live track missing");
			^false;
		};

		Buffer.read(s, path, action: { |b|
			var oldSynth, oldBuf, changed;

			if(b.notNil and: { b.numFrames > 0 }) {
				changed = force or: { b.numFrames != ~liveTrackFrames };

				if(changed) {
					oldSynth = ~liveTrackSynth;
					oldBuf = ~liveTrackBuf;
					~liveTrackBuf = b;
					~liveTrackFrames = b.numFrames;
					~liveTrackSynth = Synth.head(~liveGroup, \liveTrackTape, [
						\out, ~mixBus,
						\buf, b.bufnum,
						\amp, ~liveTrackAmp
					]);
					if(oldSynth.notNil) { try { oldSynth.set(\gate, 0) } };
					if(oldBuf.notNil) {
						Routine({
							1.6.wait;
							try { oldBuf.free };
						}).play(SystemClock);
					};
					~setStatus.("Live track refreshed: " ++ PathName.new(path).fileName);
				} {
					try { b.free };
				};
			} {
				if(b.notNil) { try { b.free } };
				~setStatus.("Live track read failed");
			};
		});

		^true;
	};

	~startLiveTrackRefresh = {
		if(~liveTrackRoutine.notNil) { try { ~liveTrackRoutine.stop } };
		~liveTrackRoutine = Routine({
			loop {
				~liveTrackRefreshSec.wait;
				~readLiveTrackBuffer.(false);
			};
		}).play(SystemClock);
		~active[\liveTrack] = ~liveTrackRoutine;
	};

	~startLiveTrack = { |path=nil|
		var resolved;

		if(path.notNil and: { path.asString.size > 0 }) {
			resolved = ~resolveLiveTrackPath.(path);
		} {
			if(~liveTrackPath.notNil and: { File.exists(~liveTrackPath) and: { PathName.new(~liveTrackPath).isFile } }) {
				resolved = ~liveTrackPath;
			} {
				resolved = ~resolveLiveTrackPath.(~sampleRoot);
			};
		};

		if(resolved.isNil) {
			~setStatus.("No live track found");
			{ if(~liveBtn.notNil) { ~liveBtn.value = 0 } }.defer;
			^false;
		};

		~liveTrackPath = resolved.asString.standardizePath;
		~readLiveTrackBuffer.(true);
		~startLiveTrackRefresh.();
		{ if(~liveBtn.notNil) { ~liveBtn.value = 1 } }.defer;
		~setStatus.("Live track following: " ++ PathName.new(~liveTrackPath).fileName);
		^true;
	};

	~updateLiveInputMonitor = {
		if(~liveInputRecorder.notNil) {
			~liveInputRecorder.set(
				\amp, ~liveInputAmp,
				\monitor, ~liveInputMonitor,
				\preLevel, ~liveInputPreLevel
			);
		};
	};

	~ensureLiveInputBuffer = {
		var frames;

		if(~liveInputBuf.isNil) {
			frames = (s.sampleRate * ~liveInputSeconds).asInteger.max(44100);
			~liveInputBuf = Buffer.alloc(s, frames, 2);
			("SC LIVE INPUT BUFFER: " ++ ~liveInputSeconds ++ " sec / " ++ frames ++ " frames").postln;
		};

		^~liveInputBuf;
	};

	~startLiveInput = {
		var buf;

		buf = ~ensureLiveInputBuffer.();

		if(~liveInputRecorder.notNil) {
			try { ~liveInputRecorder.free };
		};

		~liveInputStartedAt = Main.elapsedTime;
		~liveInputRecorder = Synth.head(~liveGroup, \liveInputRecorderTape, [
			\out, ~mixBus,
			\buf, buf.bufnum,
			\amp, ~liveInputAmp,
			\monitor, ~liveInputMonitor,
			\preLevel, ~liveInputPreLevel
		]);
		~active[\liveInput] = ~liveInputRecorder;
		{ if(~liveInBtn.notNil) { ~liveInBtn.value = 1 } }.defer;
		~setStatus.("SC live input recording");
		^true;
	};

	~liveRecentStart = {
		var elapsed, head, back;

		if(~liveInputStartedAt.isNil) {
			^rrand(0.0, 0.96);
		};

		elapsed = (Main.elapsedTime - ~liveInputStartedAt).max(0.0);
		head = (elapsed / ~liveInputSeconds.max(1.0)).wrap(0.0, 1.0);
		back = exprand(0.04, 0.85);
		^(head - back).wrap(0.0, 0.999);
	};

	~setScCutSource = { |mode|
		var raw, idx;

		raw = mode.asString.toLower;
		~scCutSourceMode = switch(
			raw,
			"live", { \live },
			"blend", { \blend },
			{ \material }
		);

		idx = [\material, \live, \blend].indexOf(~scCutSourceMode);
		if(idx.isNil) { idx = 0 };
		{
			if(~scCutSourceMenu.notNil) {
				~scCutSourceMenu.value = idx;
			};
		}.defer;
		~setStatus.("SC cut source: " ++ ~scCutSourceMode.asString);
	};

	~chooseScCutSource = {
		var materialReady, liveReady;

		materialReady = ~buffersReady.();
		liveReady = ~liveInputBuf.notNil and: { ~liveInputRecorder.notNil };

		^switch(
			~scCutSourceMode,
			\live, {
				if(liveReady) {
					\live;
				} {
					if(materialReady) { \material } { nil };
				};
			},
			\blend, {
				if(materialReady and: { liveReady }) {
					[\material, \live].wchoose([0.65, 0.35]);
				} {
					if(materialReady) {
						\material;
					} {
						if(liveReady) { \live } { nil };
					};
				};
			},
			{
				if(materialReady) {
					\material;
				} {
					if(liveReady) { \live } { nil };
				};
			}
		);
	};

	~fireMaterialCut = { |mode=\cut|
		var idx, meta, sampleDur, fragDur, start, rate, pan, hp, lp, amp, revMix;

		if(~buffersReady.().not) {
			~setStatus.("Load a WAV file/folder before material SC cuts");
			^nil;
		};

		idx = switch(
			mode,
			\phrase, { ~chooseIndex.(\phrase) },
			\glitch, { ~weightedChooseIndex.(0.95) },
			{ ~weightedChooseIndex.(0.85) }
		);
		if(idx.isNil) { idx = ~chooseIndex.(\any) };
		if(idx.isNil) { ^nil };

		meta = ~sampleMeta[idx];
		sampleDur = meta[\dur].max(0.05);
		fragDur = switch(
			mode,
			\glitch, { exprand(0.035, min(0.18, sampleDur.max(0.04))) },
			\phrase, { exprand(0.40, min(2.6, sampleDur.max(0.5))) },
			\stab, { exprand(0.06, min(0.38, sampleDur.max(0.08))) },
			{ exprand(0.08, min(0.85, sampleDur.max(0.1))) }
		);
		start = rrand(0.0, max(0.0, sampleDur - fragDur));
		rate = switch(
			mode,
			\glitch, { [0.5, 0.75, 1.0, 1.25, 1.6, -0.75, -1.0].choose },
			\phrase, { [0.75, 1.0, 1.0, 1.08, -0.6].choose },
			{ [0.5, 0.75, 1.0, 1.0, 1.25, -0.7, -1.0].choose }
		);
		pan = rrand(-0.95, 0.95);
		hp = switch(mode, \phrase, { 90 }, { [80, 140, 260, 520, 900].choose });
		lp = switch(mode, \glitch, { [1700, 2600, 4200, 7800].choose }, { [3600, 6500, 9500, 14000].choose });
		amp = exprand(0.10, 0.38) * ~liveCutAmp;
		revMix = switch(mode, \phrase, { 0.14 }, \glitch, { rrand(0.0, 0.28) }, { rrand(0.04, 0.22) });

		Synth.head(~liveCutGroup, \cutFragTape, [
			\out, ~mixBus,
			\buf, ~buffers[idx].bufnum,
			\amp, amp,
			\rate, rate,
			\start, start / sampleDur,
			\pan, pan,
			\atk, fragDur.min(0.03),
			\rel, fragDur.max(0.03),
			\hp, hp,
			\lp, lp,
			\revMix, revMix
		]);
	};

	~fireLiveInputCut = { |mode=\cut|
		var dur, start, rate, pan, hp, lp, amp, revMix;

		if(~liveInputBuf.isNil) {
			~setStatus.("Start SC INPUT before live cuts");
			^nil;
		};

		dur = switch(
			mode,
			\glitch, { exprand(0.035, 0.16) },
			\phrase, { exprand(0.45, 2.4) },
			\stab, { exprand(0.07, 0.34) },
			{ exprand(0.08, 0.75) }
		);
		start = ~liveRecentStart.();
		rate = switch(
			mode,
			\glitch, { [0.5, 0.75, 1.0, 1.25, 1.6, -0.75, -1.0].choose },
			\phrase, { [0.75, 1.0, 1.0, 1.08, -0.6].choose },
			{ [0.5, 0.75, 1.0, 1.0, 1.25, -0.7, -1.0].choose }
		);
		pan = rrand(-0.95, 0.95);
		hp = switch(mode, \phrase, { 90 }, { [80, 140, 260, 520, 900].choose });
		lp = switch(mode, \glitch, { [1700, 2600, 4200, 7800].choose }, { [3600, 6500, 9500, 14000].choose });
		amp = exprand(0.12, 0.42) * ~liveCutAmp;
		revMix = switch(mode, \phrase, { 0.16 }, \glitch, { rrand(0.0, 0.32) }, { rrand(0.05, 0.24) });

		Synth.head(~liveCutGroup, \liveInputCutTape, [
			\out, ~mixBus,
			\buf, ~liveInputBuf.bufnum,
			\amp, amp,
			\rate, rate,
			\start, start,
			\dur, dur,
			\pan, pan,
			\hp, hp,
			\lp, lp,
			\revMix, revMix
		]);
	};

	~fireScCut = { |mode=\cut|
		var source;

		source = ~chooseScCutSource.();

		if(source.isNil) {
			if((~scCutSourceMode == \live) or: { ~scCutSourceMode == \blend }) {
				~startLiveInput.();
				source = ~chooseScCutSource.();
			};
		};

		if(source.isNil) {
			~setStatus.("Load material or start SC INPUT before SC CUTS");
			^nil;
		};

		if(source == \live) {
			~fireLiveInputCut.(mode);
		} {
			~fireMaterialCut.(mode);
		};
	};

	~liveInputShot = { |mode=\stab|
		~fireScCut.(mode);
		~setStatus.("SC " ++ ~scCutSourceMode.asString ++ " " ++ mode.asString);
	};

	~startLiveCutups = {
		var source;

		if(((~scCutSourceMode == \live) or: { ~scCutSourceMode == \blend }) and: { ~liveInputRecorder.isNil }) {
			~startLiveInput.();
		};

		source = ~chooseScCutSource.();
		if(source.isNil) {
			~setStatus.("Load material or start SC INPUT before SC CUTS");
			{ if(~liveCutBtn.notNil) { ~liveCutBtn.value = 0 } }.defer;
			^false;
		};

		~hardStopLiveCutups.();

		~liveCutRoutine = Routine({
			0.45.wait;
			loop {
				var mode, repeats;

				if(~active[\liveCutups] !== thisThread) { nil.alwaysYield };

				mode = [\cut, \cut, \stab, \phrase, \glitch].wchoose([5, 4, 3, 2, 3].normalizeSum);
				if(mode == \glitch) {
					repeats = rrand(2, 6);
					repeats.do {
						~fireScCut.(\glitch);
						exprand(0.025, 0.11).wait;
					};
				} {
					~fireScCut.(mode);
				};

				switch(
					~density,
					\sparse, { exprand(0.30, 1.2).wait },
					\medium, { exprand(0.12, 0.55).wait },
					\dense,  { exprand(0.035, 0.22).wait }
				);
			};
		}).play(SystemClock);

		~active[\liveCutups] = ~liveCutRoutine;
		{ if(~liveCutBtn.notNil) { ~liveCutBtn.value = 1 } }.defer;
		~setStatus.("SC " ++ ~scCutSourceMode.asString ++ " cutups running");
		^true;
	};

	~bedWaitTime = {
		^rrand(~bedMinWait.min(~bedMaxWait), ~bedMinWait.max(~bedMaxWait)).max(0.25);
	};

	~bedPlayTime = {
		^rrand(~bedMinDur.min(~bedMaxDur), ~bedMinDur.max(~bedMaxDur)).max(1.0);
	};

	~setBedReverse = { |flag|
		~bedReverse = flag.asInteger > 0;
		{
			if(~bedReverseBtn.notNil) {
				~bedReverseBtn.value = ~bedReverse.binaryValue;
			};
		}.defer;
		~setStatus.("Bed reverse: " ++ ~bedReverse.asString);
	};

	~fireBedLoop = {
		var source, idx, meta, sampleDur, playDur, windowDur, windowMin, windowMax, startSec, loopStart, loopEnd, pan, rate, rev;

		source = ~chooseScCutSource.();

		if(source.isNil) {
			if((~scCutSourceMode == \live) or: { ~scCutSourceMode == \blend }) {
				~startLiveInput.();
				source = ~chooseScCutSource.();
			};
		};

		if(source.isNil) {
			~setStatus.("Load material or start SC INPUT before BED LOOPS");
			^nil;
		};

		playDur = ~bedPlayTime.();
		pan = rrand(-0.85, 0.85);
		rate = [0.5, 0.75, 1.0, 1.0, 1.08, 1.15].choose;
		rev = (~bedReverse or: { 0.16.coin }).binaryValue;

		if(source == \live) {
			if(~liveInputBuf.isNil) {
				~startLiveInput.();
			};
			sampleDur = ~liveInputSeconds.max(1.0);
			windowMax = sampleDur.min(8.0).max(0.5);
			windowMin = 1.2.min(windowMax).max(0.1);
			windowDur = rrand(windowMin, windowMax);
			loopStart = ~liveRecentStart.().min(1.0 - (windowDur / sampleDur).clip(0.02, 0.95)).max(0.0);
			loopEnd = (loopStart + (windowDur / sampleDur)).clip(0.01, 1.0);

			Synth.head(~bedGroup, \liveBedLoopTape, [
				\out, ~mixBus,
				\buf, ~liveInputBuf.bufnum,
				\amp, ~bedAmp,
				\rate, rate,
				\loopStart, loopStart,
				\loopEnd, loopEnd,
				\playDur, playDur,
				\pan, pan,
				\reverse, rev,
				\dropout, ~bedDropout
			]);
		} {
			if(~buffersReady.().not) {
				~setStatus.("Load material before material BED LOOPS");
				^nil;
			};
			idx = ~chooseIndex.([\long, \phrase, \short, \any].wchoose([4, 4, 2, 1].normalizeSum));
			if(idx.isNil) { idx = ~chooseIndex.(\any) };
			if(idx.isNil) { ^nil };

			meta = ~sampleMeta[idx];
			sampleDur = meta[\dur].max(0.1);
			windowMax = sampleDur.min(12.0).max(0.25);
			windowMin = 1.5.min(windowMax).max(0.1);
			windowDur = rrand(windowMin, windowMax);
			startSec = rrand(0.0, (sampleDur - windowDur).max(0.0));
			loopStart = (startSec / sampleDur).clip(0.0, 0.998);
			loopEnd = ((startSec + windowDur) / sampleDur).clip(0.001, 1.0);

			Synth.head(~bedGroup, \bedLoopTape, [
				\out, ~mixBus,
				\buf, ~buffers[idx].bufnum,
				\amp, ~bedAmp,
				\rate, rate,
				\loopStart, loopStart,
				\loopEnd, loopEnd,
				\playDur, playDur,
				\pan, pan,
				\reverse, rev,
				\dropout, ~bedDropout
			]);
		};
	};

	~startBedLoops = {
		var source;

		if((~scCutSourceMode == \live) or: { ~scCutSourceMode == \blend }) {
			if(~liveInputRecorder.isNil) {
				~startLiveInput.();
			};
		};

		source = ~chooseScCutSource.();
		if(source.isNil) {
			~setStatus.("Load material or start SC INPUT before BED LOOPS");
			{ if(~bedBtn.notNil) { ~bedBtn.value = 0 } }.defer;
			^false;
		};

		~hardStopBedLoops.();

		~bedRoutine = Routine({
			0.3.wait;
			loop {
				if(~active[\bedLoops] !== thisThread) { nil.alwaysYield };
				~fireBedLoop.();
				~bedWaitTime.().wait;
			};
		}).play(SystemClock);

		~active[\bedLoops] = ~bedRoutine;
		{ if(~bedBtn.notNil) { ~bedBtn.value = 1 } }.defer;
		~setStatus.("Bed loops running: " ++ ~scCutSourceMode.asString);
		^true;
	};

	// ============================================================
	// ENGINES
	// ============================================================

	~startCuts = {
		if(~canStartPlayback.(~cutsBtn).not) {
			^false;
		};

		~hardStopCuts.();

		~active[\cuts] = Routine({
			inf.do {
				var idx, meta, dur, fragDur, start, rate, pan, hp, lp, amp, revMix;

				if(~active[\cuts] !== thisThread) { nil.alwaysYield };

				if(~cutsMuted.not and: { ~buffers.notEmpty }) {
					idx = ~weightedChooseIndex.(0.85);

					if(idx.notNil) {
						meta = ~sampleMeta[idx];
						dur = meta[\dur].max(0.05);
						fragDur = exprand(0.04, min(1.0, dur.max(0.06)));
						start = rrand(0.0, max(0.0, dur - fragDur));
						rate = [0.5, 0.75, 1.0, 1.0, 1.2, 1.4, -0.7, -1.0].choose;
						pan = rrand(-0.95, 0.95);
						hp = [80, 120, 240, 500, 900].choose;
						lp = [2500, 4000, 7000, 10000, 14000].choose;
						amp = exprand(0.08, 0.38) * ~cutsAmp;
						revMix = rrand(0.0, 0.3);

						Synth.head(~cutsGroup, \cutFragTape, [
							\out, ~mixBus,
							\buf, ~buffers[idx].bufnum,
							\amp, amp,
							\rate, rate,
							\start, start / dur,
							\pan, pan,
							\atk, fragDur.min(0.03),
							\rel, fragDur.max(0.03),
							\hp, hp,
							\lp, lp,
							\revMix, revMix
						]);
					};
				};

				switch(
					~density,
					\sparse, { exprand(0.15, 0.42).wait },
					\medium, { exprand(0.04, 0.22).wait },
					\dense,  { exprand(0.01, 0.09).wait }
				);
			};
		}).play(SystemClock);

		~setStatus.("Cuts running");
		^true;
	};

	~startLoops = { |numVoices = 4|
		var voices;

		if(~canStartPlayback.(~loopsBtn).not) {
			^false;
		};

		~hardStopLoops.();

		voices = numVoices.collect {
			var idx, dur, cellLen, start, finish;

			idx = ~chooseIndex.([\micro, \short, \phrase].wchoose([6, 4, 2].normalizeSum));
			if(idx.isNil) { idx = ~chooseIndex.(\any) };
			if(idx.isNil) { ^nil };

			dur = ~sampleMeta[idx][\dur].max(0.1);
			cellLen = exprand(0.04, min(0.6, dur * 0.6));
			start = rrand(0.0, max(0.0, dur - cellLen));
			finish = (start + cellLen).min(dur);

			Synth.head(~loopsGroup, \loopCellTape, [
				\out, ~mixBus,
				\buf, ~buffers[idx].bufnum,
				\amp, exprand(0.05, 0.24) * ~loopsAmp * (~loopsMuted.not.binaryValue),
				\rate, [0.5, 0.75, 1.0, 1.25, -1.0].choose,
				\start, start / dur,
				\end, finish / dur,
				\pan, rrand(-1.0, 1.0),
				\hp, [100, 200, 400, 800].choose,
				\lp, [1800, 3000, 6000, 9000].choose,
				\trigRate, exprand(1.5, 10.0)
			]);
		}.reject(_.isNil);

		~active[\loops] = voices;
		~setStatus.("Loops running");
		^true;
	};

	~startGhosts = { |numVoices = 3|
		var voices;

		if(~canStartPlayback.(~ghostsBtn).not) {
			^false;
		};

		~hardStopGhosts.();

		voices = numVoices.collect {
			var idx;

			idx = ~chooseIndex.(\phrase);
			if(idx.isNil) { idx = ~chooseIndex.(\long) };
			if(idx.isNil) { idx = ~chooseIndex.(\any) };
			if(idx.isNil) { ^nil };

			Synth.head(~ghostsGroup, \ghostPlayerTape, [
				\out, ~mixBus,
				\buf, ~buffers[idx].bufnum,
				\amp, exprand(0.04, 0.12) * ~ghostsAmp * (~ghostsMuted.not.binaryValue),
				\rate, [-0.25, -0.5, -0.75, -1.0].choose,
				\start, rrand(0.25, 1.0),
				\pan, rrand(-1.0, 1.0),
				\hp, [140, 240, 400, 700].choose,
				\lp, [1200, 2200, 3600, 5000].choose,
				\rel, exprand(2.0, 7.0)
			]);
		}.reject(_.isNil);

		~active[\ghosts] = voices;
		~setStatus.("Ghosts running");
		^true;
	};

	~startCloud = { |numVoices = 4|
		var voices;

		if(~canStartPlayback.(~cloudBtn).not) {
			^false;
		};

		~hardStopCloud.();

		voices = numVoices.collect {
			var idx;
			idx = ~weightedChooseIndex.(0.9);
			if(idx.isNil) { ^nil };

			Synth.head(~cloudGroup, \grainBufCloudTape, [
				\out, ~mixBus,
				\buf, ~buffers[idx].bufnum,
				\amp, exprand(0.03, 0.12) * ~cloudAmp * (~cloudMuted.not.binaryValue),
				\dens, rrand(7, 24),
				\posRate, exprand(0.03, 0.6),
				\rate, [0.5, 0.75, 1.0, 1.25, -0.5].choose,
				\gdur, exprand(0.03, 0.18),
				\panWidth, rrand(0.3, 1.0),
				\hp, [80, 140, 220, 500].choose,
				\lp, [2500, 5000, 8000, 12000].choose
			]);
		}.reject(_.isNil);

		~active[\cloud] = voices;
		~setStatus.("Cloud running");
		^true;
	};

	~startScene = {
		var sceneRoutine;

		("SCENE CHECK -> buffers: " ++ ~buffers.size ++ " sampleMeta: " ++ ~sampleMeta.size).postln;

		if(~canStartPlayback.(~sceneBtn).not) {
			^false;
		};

		~stopSceneOnly.();

		sceneRoutine = Routine({
			loop {
				var kind, dur;

				if(~active[\scene] !== thisThread) { nil.alwaysYield };

				kind = [\cuts, \loops, \ghosts, \cloud, \hybrid].wchoose([4, 4, 2, 2, 5].normalizeSum);
				dur = rrand(6.0, 20.0);

				~hardStopCuts.();
				~hardStopLoops.();
				~hardStopGhosts.();
				~hardStopCloud.();

				switch(
					kind,
					\cuts, {
						~startCuts.();
						dur.wait;
					},
					\loops, {
						~startLoops.(~loopVoices);
						dur.wait;
					},
					\ghosts, {
						~startGhosts.(~ghostVoices);
						dur.wait;
					},
					\cloud, {
						~startCloud.(~cloudVoices);
						dur.wait;
					},
					\hybrid, {
						~startLoops.(~loopVoices);
						0.7.wait;
						~startGhosts.(~ghostVoices);
						1.0.wait;
						~startCuts.();
						if(0.6.coin) {
							1.2.wait;
							~startCloud.(~cloudVoices);
						};
						dur.wait;
					}
				);

				~hardStopCuts.();
				~hardStopLoops.();
				~hardStopGhosts.();
				~hardStopCloud.();

				rrand(0.4, 2.4).wait;
			};
		}).play(SystemClock);

		~active[\scene] = sceneRoutine;
		~setStatus.("Scene running");
		^true;
	};

	~stab = {
		var idx, meta, dur, fragDur, start;

		if(~canStartPlayback.().not) {
			^nil;
		};

		idx = ~weightedChooseIndex.(0.9);
		if(idx.isNil) { ^nil };

		meta = ~sampleMeta[idx];
		dur = meta[\dur].max(0.05);
		fragDur = exprand(0.03, min(0.3, dur.max(0.04)));
		start = rrand(0.0, max(0.0, dur - fragDur));

		Synth.head(~cutsGroup, \cutFragTape, [
			\out, ~mixBus,
			\buf, ~buffers[idx].bufnum,
			\amp, exprand(0.15, 0.55) * ~cutsAmp,
			\rate, [1.0, 1.25, 1.5, -1.0].choose,
			\start, start / dur,
			\pan, rrand(-1.0, 1.0),
			\atk, 0.001,
			\rel, fragDur,
			\hp, [180, 500, 1200].choose,
			\lp, [2500, 4500, 9000].choose,
			\revMix, rrand(0.0, 0.2)
		]);

		~setStatus.("Stab fired");
	};

	~phraseShot = {
		var idx, meta, dur, fragDur, start;

		if(~canStartPlayback.().not) {
			^nil;
		};

		idx = ~chooseIndex.(\phrase);
		if(idx.isNil) { idx = ~chooseIndex.(\any) };
		if(idx.isNil) { ^nil };

		meta = ~sampleMeta[idx];
		dur = meta[\dur].max(0.05);
		fragDur = exprand(0.25, min(3.0, dur.max(0.3)));
		start = rrand(0.0, max(0.0, dur - fragDur));

		Synth.head(~cutsGroup, \cutFragTape, [
			\out, ~mixBus,
			\buf, ~buffers[idx].bufnum,
			\amp, 0.3 * ~cutsAmp,
			\rate, [0.75, 1.0, 1.0, 1.2, -0.7].choose,
			\start, start / dur,
			\pan, rrand(-0.7, 0.7),
			\atk, 0.01,
			\rel, fragDur,
			\hp, 110,
			\lp, 6500,
			\revMix, 0.1
		]);

		~setStatus.("Phrase fired");
	};

	~concreteShot = {
		var idx, meta, dur, fragDur, start;

		if(~canStartPlayback.().not) {
			^nil;
		};

		idx = ~weightedChooseIndex.(0.95);
		if(idx.isNil) { ^nil };

		meta = ~sampleMeta[idx];
		dur = meta[\dur].max(0.05);
		fragDur = exprand(0.08, min(1.8, dur.max(0.1)));
		start = rrand(0.0, max(0.0, dur - fragDur));

		Synth.head(~cutsGroup, \concreteCutTape, [
			\out, ~mixBus,
			\buf, ~buffers[idx].bufnum,
			\amp, exprand(0.12, 0.42) * ~cutsAmp,
			\rate, [0.5, 0.75, 1.0, 1.15, 1.4, -0.5, -0.8, -1.0].choose,
			\start, start / dur,
			\dur, fragDur,
			\pan, rrand(-0.95, 0.95),
			\fx, rrand(0.35, 0.95),
			\hp, [90, 180, 360, 720].choose,
			\lp, [1800, 3200, 5600, 9000, 12000].choose
		]);

		~setStatus.("Concrete cut fired");
	};

	~recordStart = {
		s.prepareForRecord(~recordPath);
		s.record;
		~setStatus.("Recording...");
	};

	~recordStop = {
		s.stopRecording;
		~setStatus.("Recording stopped");
	};

	~installOSC = {
		OSCdef(\cutupsTapedeckStab, { ~stab.(); }, "/cutups/stab");
		OSCdef(\cutupsTapedeckPhrase, { ~phraseShot.(); }, "/cutups/phrase");
		OSCdef(\cutupsTapedeckConcrete, { ~concreteShot.(); }, "/cutups/concrete");
		OSCdef(\cutupsTapedeckStop, {
			~stopLoader.();
			~stopAll.();
			~resetButtons.();
		}, "/cutups/stop");
		OSCdef(\cutupsTapedeckScene, { |msg|
			var on;
			on = if(msg.size > 1) { msg[1].asInteger > 0 } { true };
			if(on) {
				~startScene.();
				{ if(~sceneBtn.notNil) { ~sceneBtn.value = 1 } }.defer;
			} {
				~stopSceneOnly.();
				{ if(~sceneBtn.notNil) { ~sceneBtn.value = 0 } }.defer;
			};
		}, "/cutups/scene");
		OSCdef(\cutupsTapedeckLoadLive, { |msg|
			if(msg.size > 1) { ~sampleRoot = msg[1].asString };
			~loadMode = \live;
			~loadSamplesAsync.();
		}, "/cutups/loadLive");
		OSCdef(\cutupsTapedeckLoadChunks, { |msg|
			if(msg.size > 1) { ~sampleRoot = msg[1].asString };
			~loadMode = \chunks;
			~loadSamplesAsync.();
		}, "/cutups/loadChunks");
		OSCdef(\cutupsTapedeckChunksRefresh, {
			~refreshChunksOnce.();
		}, "/cutups/chunks/refresh");
		OSCdef(\cutupsTapedeckLiveStart, { |msg|
			if(msg.size > 1) {
				~startLiveTrack.(msg[1].asString);
			} {
				~startLiveTrack.();
			};
		}, "/cutups/live/start");
		OSCdef(\cutupsTapedeckLiveStop, {
			~hardStopLiveTrack.();
			~setStatus.("Live track stopped");
		}, "/cutups/live/stop");
		OSCdef(\cutupsTapedeckLiveAmp, { |msg|
			if(msg.size > 1) {
				~liveTrackAmp = msg[1].asFloat.clip(0, 1.5);
				~updateLiveTrackAmp.();
				~setStatus.("Live track amp: " ++ ~liveTrackAmp.round(0.001));
			};
		}, "/cutups/live/amp");
		OSCdef(\cutupsTapedeckScLiveInputStart, {
			~startLiveInput.();
		}, "/cutups/scLive/input/start");
		OSCdef(\cutupsTapedeckScLiveInputStop, {
			~hardStopLiveInput.();
			~setStatus.("SC live input stopped");
		}, "/cutups/scLive/input/stop");
		OSCdef(\cutupsTapedeckScLiveCutsStart, {
			~startLiveCutups.();
		}, "/cutups/scLive/cuts/start");
		OSCdef(\cutupsTapedeckScLiveCutsStop, {
			~hardStopLiveCutups.();
			~setStatus.("SC live cutups stopped");
		}, "/cutups/scLive/cuts/stop");
		OSCdef(\cutupsTapedeckScLiveStab, {
			~liveInputShot.(\stab);
		}, "/cutups/scLive/stab");
		OSCdef(\cutupsTapedeckScLiveGlitch, {
			~liveInputShot.(\glitch);
		}, "/cutups/scLive/glitch");
		OSCdef(\cutupsTapedeckScLiveMonitor, { |msg|
			if(msg.size > 1) {
				~liveInputMonitor = msg[1].asFloat.clip(0, 1.5);
				~updateLiveInputMonitor.();
				~setStatus.("SC input monitor: " ++ ~liveInputMonitor.round(0.001));
			};
		}, "/cutups/scLive/monitor");
		OSCdef(\cutupsTapedeckScLiveCutAmp, { |msg|
			if(msg.size > 1) {
				~liveCutAmp = msg[1].asFloat.clip(0, 1.5);
				~setStatus.("SC live cut amp: " ++ ~liveCutAmp.round(0.001));
			};
		}, "/cutups/scLive/cutAmp");
		OSCdef(\cutupsTapedeckScLiveSource, { |msg|
			if(msg.size > 1) {
				~setScCutSource.(msg[1].asString);
			};
		}, "/cutups/scLive/source");
		OSCdef(\cutupsTapedeckScLiveBedStart, {
			~startBedLoops.();
		}, "/cutups/scLive/bed/start");
		OSCdef(\cutupsTapedeckScLiveBedStop, {
			~hardStopBedLoops.();
			~setStatus.("Bed loops stopped");
		}, "/cutups/scLive/bed/stop");
		OSCdef(\cutupsTapedeckScLiveBedReverse, { |msg|
			if(msg.size > 1) {
				~setBedReverse.(msg[1]);
			} {
				~setBedReverse.(~bedReverse.not.binaryValue);
			};
		}, "/cutups/scLive/bed/reverse");
		OSCdef(\cutupsTapedeckScLiveBedAmp, { |msg|
			if(msg.size > 1) {
				~bedAmp = msg[1].asFloat.clip(0, 1.5);
				~setStatus.("Bed amp: " ++ ~bedAmp.round(0.001));
			};
		}, "/cutups/scLive/bed/amp");
		OSCdef(\cutupsTapedeckScLiveBedTiming, { |msg|
			if(msg.size > 4) {
				~bedMinWait = msg[1].asFloat.max(0.25);
				~bedMaxWait = msg[2].asFloat.max(0.25);
				~bedMinDur = msg[3].asFloat.max(1.0);
				~bedMaxDur = msg[4].asFloat.max(1.0);
				~setStatus.("Bed timing updated");
			};
		}, "/cutups/scLive/bed/timing");
		OSCdef(\cutupsTapedeckMasterAge, { |msg|
			if(msg.size > 1) {
				~ageAmt = msg[1].asFloat.clip(0, 1);
				~updateMasterFX.();
				~setStatus.("OSC age: " ++ ~ageAmt.round(0.001));
			};
		}, "/cutups/master/age");
		"OSC ready: /cutups/stab /cutups/phrase /cutups/concrete /cutups/scene /cutups/stop /cutups/loadLive /cutups/loadChunks /cutups/chunks/refresh /cutups/live/start /cutups/live/stop /cutups/live/amp /cutups/scLive/input/start /cutups/scLive/input/stop /cutups/scLive/cuts/start /cutups/scLive/cuts/stop /cutups/scLive/stab /cutups/scLive/glitch /cutups/scLive/monitor /cutups/scLive/cutAmp /cutups/scLive/source /cutups/scLive/bed/start /cutups/scLive/bed/stop /cutups/scLive/bed/reverse /cutups/scLive/bed/amp /cutups/scLive/bed/timing /cutups/master/age".postln;
	};

	~installOSC.();

	// ============================================================
	// UI
	// ============================================================

	makeUI = {
		var win, pathField, densityMenu;
		var cloudVoicesBox, loopVoicesBox, ghostVoicesBox;
		var masterSlider, driveSlider, wowSlider, flutterSlider, dropoutSlider, dirtSlider, ageSlider, dubSlider, lpSlider, hpSlider;
		var cutsSlider, loopsSlider, ghostsSlider, cloudSlider;
		var cutsMuteBtn, loopsMuteBtn, ghostsMuteBtn, cloudMuteBtn;
		var liveAmpSlider, liveInputMonitorSlider, liveCutAmpSlider, bedAmpSlider;
		var bedWaitMinBox, bedWaitMaxBox, bedDurMinBox, bedDurMaxBox;
		var scCutSourceMenu;

		win = Window("TRANSMISSIONS TAPE-DECK — DIRECT LOAD", Rect(60, 60, 1140, 900)).front;
		win.alwaysOnTop = true;

		StaticText(win, Rect(20, 15, 100, 20)).string = "SOURCE";

		pathField = TextField(win, Rect(110, 12, 555, 24));
		pathField.string = ~sampleRoot;

		Button(win, Rect(675, 12, 55, 24))
			.states_([["SET"]])
			.action_({
				~sampleRoot = pathField.string;
				~setStatus.("Path set — choose LOAD DIR/LIVE/CHUNKS");
			});

		Button(win, Rect(740, 12, 80, 24))
			.states_([["LOAD DIR"]])
			.action_({
				~sampleRoot = pathField.string;
				~loadMode = \folder;
				~loadSamplesAsync.();
			});

		Button(win, Rect(830, 12, 90, 24))
			.states_([["LOAD LIVE"]])
			.action_({
				~sampleRoot = pathField.string;
				~loadMode = \live;
				~loadSamplesAsync.();
			});

		Button(win, Rect(930, 12, 105, 24))
			.states_([["LOAD CHUNKS"]])
			.action_({
				~sampleRoot = pathField.string;
				~loadMode = \chunks;
				~loadSamplesAsync.();
			});

		Button(win, Rect(1045, 12, 65, 24))
			.states_([["STOP"]])
			.action_({
				~stopAll.();
			});

		~countText = StaticText(win, Rect(20, 48, 260, 20));
		~countText.string = "Loaded: 0";

		~statusText = StaticText(win, Rect(290, 48, 820, 20));
		~statusText.string = "Ready";

		StaticText(win, Rect(20, 72, 90, 18)).string = "LOAD PROG";

		~progressBar = Slider(win, Rect(110, 72, 1000, 18));
		~progressBar.orientation = \horizontal;
		~progressBar.enabled = false;
		~progressBar.value = 0;

		StaticText(win, Rect(20, 108, 90, 20)).string = "Density";

		densityMenu = PopUpMenu(win, Rect(90, 106, 120, 24));
		densityMenu.items = ["sparse", "medium", "dense"];
		densityMenu.value = 1;
		densityMenu.action = {
			~density = [\sparse, \medium, \dense][densityMenu.value];
			~setStatus.("Density: " ++ ~density.asString);
		};

		StaticText(win, Rect(240, 108, 100, 20)).string = "Cloud Voices";
		cloudVoicesBox = NumberBox(win, Rect(335, 106, 55, 24));
		cloudVoicesBox.value = ~cloudVoices;
		cloudVoicesBox.action = { ~cloudVoices = cloudVoicesBox.value.asInteger.max(1) };

		StaticText(win, Rect(420, 108, 100, 20)).string = "Loop Voices";
		loopVoicesBox = NumberBox(win, Rect(510, 106, 55, 24));
		loopVoicesBox.value = ~loopVoices;
		loopVoicesBox.action = { ~loopVoices = loopVoicesBox.value.asInteger.max(1) };

		StaticText(win, Rect(595, 108, 100, 20)).string = "Ghost Voices";
		ghostVoicesBox = NumberBox(win, Rect(690, 106, 55, 24));
		ghostVoicesBox.value = ~ghostVoices;
		ghostVoicesBox.action = { ~ghostVoices = ghostVoicesBox.value.asInteger.max(1) };

		Button(win, Rect(780, 104, 90, 28))
			.states_([["REC IN"]])
			.action_({ ~recordStart.() });

		Button(win, Rect(880, 104, 90, 28))
			.states_([["REC OUT"]])
			.action_({ ~recordStop.() });

		Button(win, Rect(980, 104, 50, 28))
			.states_([["STAB"]])
			.action_({ ~stab.() });

		Button(win, Rect(1038, 104, 50, 28))
			.states_([["PHRS"]])
			.action_({ ~phraseShot.() });

		Button(win, Rect(1096, 104, 44, 28))
			.states_([["CNCR"]])
			.action_({ ~concreteShot.() });

		StaticText(win, Rect(20, 150, 180, 20)).string = "MASTER TAPE STRIP";

		StaticText(win, Rect(20, 175, 60, 18)).string = "OUT";
		masterSlider = Slider(win, Rect(25, 200, 28, 210));
		masterSlider.orientation = \vertical;
		masterSlider.value = ~masterAmp.linlin(0, 2, 0, 1);
		masterSlider.action = {
			~masterAmp = masterSlider.value.linlin(0, 1, 0.0, 2.0);
			~updateMasterFX.();
		};

		StaticText(win, Rect(75, 175, 60, 18)).string = "DRIVE";
		driveSlider = Slider(win, Rect(80, 200, 28, 210));
		driveSlider.orientation = \vertical;
		driveSlider.value = ~driveAmt.linlin(1.0, 3.0, 0, 1);
		driveSlider.action = {
			~driveAmt = driveSlider.value.linlin(0, 1, 1.0, 3.0);
			~updateMasterFX.();
		};

		StaticText(win, Rect(130, 175, 60, 18)).string = "WOW";
		wowSlider = Slider(win, Rect(135, 200, 28, 210));
		wowSlider.orientation = \vertical;
		wowSlider.value = ~wowAmt.linlin(0, 0.08, 0, 1);
		wowSlider.action = {
			~wowAmt = wowSlider.value.linlin(0, 1, 0.0, 0.08);
			~updateMasterFX.();
		};

		StaticText(win, Rect(185, 175, 60, 18)).string = "FLUT";
		flutterSlider = Slider(win, Rect(190, 200, 28, 210));
		flutterSlider.orientation = \vertical;
		flutterSlider.value = ~flutterAmt.linlin(0, 0.04, 0, 1);
		flutterSlider.action = {
			~flutterAmt = flutterSlider.value.linlin(0, 1, 0.0, 0.04);
			~updateMasterFX.();
		};

		StaticText(win, Rect(240, 175, 60, 18)).string = "DROP";
		dropoutSlider = Slider(win, Rect(245, 200, 28, 210));
		dropoutSlider.orientation = \vertical;
		dropoutSlider.value = ~dropoutAmt.linlin(0, 0.4, 0, 1);
		dropoutSlider.action = {
			~dropoutAmt = dropoutSlider.value.linlin(0, 1, 0.0, 0.4);
			~updateMasterFX.();
		};

		StaticText(win, Rect(295, 175, 60, 18)).string = "DIRT";
		dirtSlider = Slider(win, Rect(300, 200, 28, 210));
		dirtSlider.orientation = \vertical;
		dirtSlider.value = ~dirtAmt.linlin(0, 0.5, 0, 1);
		dirtSlider.action = {
			~dirtAmt = dirtSlider.value.linlin(0, 1, 0.0, 0.5);
			~updateMasterFX.();
		};

		StaticText(win, Rect(350, 175, 60, 18)).string = "AGE";
		ageSlider = Slider(win, Rect(355, 200, 28, 210));
		ageSlider.orientation = \vertical;
		ageSlider.value = ~ageAmt.clip(0, 1);
		ageSlider.action = {
			~ageAmt = ageSlider.value.clip(0, 1);
			~updateMasterFX.();
		};

		StaticText(win, Rect(405, 175, 60, 18)).string = "DUB";
		dubSlider = Slider(win, Rect(410, 200, 28, 210));
		dubSlider.orientation = \vertical;
		dubSlider.value = ~dubAmt.linlin(0, 0.8, 0, 1);
		dubSlider.action = {
			~dubAmt = dubSlider.value.linlin(0, 1, 0.0, 0.8);
			~updateMasterFX.();
		};

		StaticText(win, Rect(460, 175, 60, 18)).string = "LP";
		lpSlider = Slider(win, Rect(465, 200, 28, 210));
		lpSlider.orientation = \vertical;
		lpSlider.value = ~lpFreq.linlin(1500, 14000, 0, 1);
		lpSlider.action = {
			~lpFreq = lpSlider.value.linlin(0, 1, 1500, 14000);
			~updateMasterFX.();
		};

		StaticText(win, Rect(515, 175, 60, 18)).string = "HP";
		hpSlider = Slider(win, Rect(520, 200, 28, 210));
		hpSlider.orientation = \vertical;
		hpSlider.value = ~hpFreq.linlin(20, 1500, 0, 1);
		hpSlider.action = {
			~hpFreq = hpSlider.value.linlin(0, 1, 20, 1500);
			~updateMasterFX.();
		};

		StaticText(win, Rect(560, 150, 260, 20)).string = "LAYER DECKS";

		StaticText(win, Rect(560, 175, 60, 18)).string = "CUTS";
		cutsSlider = Slider(win, Rect(565, 200, 28, 210));
		cutsSlider.orientation = \vertical;
		cutsSlider.value = ~cutsAmp;
		cutsSlider.action = { ~cutsAmp = cutsSlider.value };

		cutsMuteBtn = Button(win, Rect(550, 420, 60, 24))
			.states_([["MUTE"], ["CUT"]])
			.action_({ ~cutsMuted = (cutsMuteBtn.value == 1) });

		~cutsBtn = Button(win, Rect(540, 455, 80, 30))
			.states_([["START"], ["RUN"]])
			.action_({
				if(~cutsBtn.value == 1) {
					~startCuts.();
				} {
					~hardStopCuts.();
					~setStatus.("Cuts stopped");
				};
			});

		StaticText(win, Rect(640, 175, 60, 18)).string = "LOOPS";
		loopsSlider = Slider(win, Rect(645, 200, 28, 210));
		loopsSlider.orientation = \vertical;
		loopsSlider.value = ~loopsAmp;
		loopsSlider.action = { ~loopsAmp = loopsSlider.value };

		loopsMuteBtn = Button(win, Rect(630, 420, 60, 24))
			.states_([["MUTE"], ["CUT"]])
			.action_({ ~loopsMuted = (loopsMuteBtn.value == 1) });

		~loopsBtn = Button(win, Rect(620, 455, 80, 30))
			.states_([["START"], ["RUN"]])
			.action_({
				if(~loopsBtn.value == 1) {
					~startLoops.(~loopVoices);
				} {
					~hardStopLoops.();
					~setStatus.("Loops stopped");
				};
			});

		StaticText(win, Rect(720, 175, 60, 18)).string = "GHOST";
		ghostsSlider = Slider(win, Rect(725, 200, 28, 210));
		ghostsSlider.orientation = \vertical;
		ghostsSlider.value = ~ghostsAmp;
		ghostsSlider.action = { ~ghostsAmp = ghostsSlider.value };

		ghostsMuteBtn = Button(win, Rect(710, 420, 60, 24))
			.states_([["MUTE"], ["CUT"]])
			.action_({ ~ghostsMuted = (ghostsMuteBtn.value == 1) });

		~ghostsBtn = Button(win, Rect(700, 455, 80, 30))
			.states_([["START"], ["RUN"]])
			.action_({
				if(~ghostsBtn.value == 1) {
					~startGhosts.(~ghostVoices);
				} {
					~hardStopGhosts.();
					~setStatus.("Ghosts stopped");
				};
			});

		StaticText(win, Rect(800, 175, 60, 18)).string = "CLOUD";
		cloudSlider = Slider(win, Rect(805, 200, 28, 210));
		cloudSlider.orientation = \vertical;
		cloudSlider.value = ~cloudAmp;
		cloudSlider.action = { ~cloudAmp = cloudSlider.value };

		cloudMuteBtn = Button(win, Rect(790, 420, 60, 24))
			.states_([["MUTE"], ["CUT"]])
			.action_({ ~cloudMuted = (cloudMuteBtn.value == 1) });

		~cloudBtn = Button(win, Rect(780, 455, 80, 30))
			.states_([["START"], ["RUN"]])
			.action_({
				if(~cloudBtn.value == 1) {
					~startCloud.(~cloudVoices);
				} {
					~hardStopCloud.();
					~setStatus.("Cloud stopped");
				};
			});

		StaticText(win, Rect(900, 175, 180, 18)).string = "TRANSPORT / LIVE";

		~sceneBtn = Button(win, Rect(900, 210, 170, 36))
			.states_([["SCENE AUTO"], ["SCENE ON"]])
			.action_({
				if(~sceneBtn.value == 1) {
					~startScene.();
				} {
					~stopSceneOnly.();
					~hardStopCuts.();
					~hardStopLoops.();
					~hardStopGhosts.();
					~hardStopCloud.();
					~setStatus.("Scene stopped");
				};
			});

		~liveBtn = Button(win, Rect(900, 260, 170, 32))
			.states_([["LIVE TRACK"], ["LIVE ON"]])
			.action_({
				if(~liveBtn.value == 1) {
					~startLiveTrack.(pathField.string);
				} {
					~hardStopLiveTrack.();
					~setStatus.("Live track stopped");
				};
			});

		StaticText(win, Rect(900, 302, 70, 18)).string = "LIVE AMP";
		liveAmpSlider = Slider(win, Rect(970, 304, 100, 18));
		liveAmpSlider.orientation = \horizontal;
		liveAmpSlider.value = ~liveTrackAmp.clip(0, 1);
		liveAmpSlider.action = {
			~liveTrackAmp = liveAmpSlider.value.linlin(0, 1, 0.0, 1.5);
			~updateLiveTrackAmp.();
		};

		~liveInBtn = Button(win, Rect(900, 335, 82, 32))
			.states_([["SC INPUT"], ["INPUT ON"]])
			.action_({
				if(~liveInBtn.value == 1) {
					~startLiveInput.();
				} {
					~hardStopLiveInput.();
					~setStatus.("SC live input stopped");
				};
			});

		~liveCutBtn = Button(win, Rect(988, 335, 82, 32))
			.states_([["SC CUTS"], ["CUTS ON"]])
			.action_({
				if(~liveCutBtn.value == 1) {
					~startLiveCutups.();
				} {
					~hardStopLiveCutups.();
					~setStatus.("SC live cutups stopped");
				};
			});

		~bedBtn = Button(win, Rect(900, 375, 82, 28))
			.states_([["BED LOOP"], ["BED ON"]])
			.action_({
				if(~bedBtn.value == 1) {
					~startBedLoops.();
				} {
					~hardStopBedLoops.();
					~setStatus.("Bed loops stopped");
				};
			});

		~bedReverseBtn = Button(win, Rect(988, 375, 82, 28))
			.states_([["BED FWD"], ["BED REV"]])
			.action_({ ~setBedReverse.(~bedReverseBtn.value) });

		Button(win, Rect(900, 411, 82, 28))
			.states_([["LIVE STAB"]])
			.action_({ ~liveInputShot.(\stab) });

		Button(win, Rect(988, 411, 82, 28))
			.states_([["LIVE GLCH"]])
			.action_({ ~liveInputShot.(\glitch) });

		StaticText(win, Rect(900, 450, 58, 18)).string = "IN MON";
		liveInputMonitorSlider = Slider(win, Rect(960, 452, 110, 18));
		liveInputMonitorSlider.orientation = \horizontal;
		liveInputMonitorSlider.value = ~liveInputMonitor.clip(0, 1);
		liveInputMonitorSlider.action = {
			~liveInputMonitor = liveInputMonitorSlider.value.linlin(0, 1, 0.0, 1.5);
			~updateLiveInputMonitor.();
		};

		StaticText(win, Rect(900, 476, 58, 18)).string = "CUT AMP";
		liveCutAmpSlider = Slider(win, Rect(960, 478, 110, 18));
		liveCutAmpSlider.orientation = \horizontal;
		liveCutAmpSlider.value = ~liveCutAmp.clip(0, 1);
		liveCutAmpSlider.action = {
			~liveCutAmp = liveCutAmpSlider.value.linlin(0, 1, 0.0, 1.5);
		};

		StaticText(win, Rect(900, 502, 58, 18)).string = "BED AMP";
		bedAmpSlider = Slider(win, Rect(960, 504, 110, 18));
		bedAmpSlider.orientation = \horizontal;
		bedAmpSlider.value = ~bedAmp.clip(0, 1);
		bedAmpSlider.action = {
			~bedAmp = bedAmpSlider.value.linlin(0, 1, 0.0, 1.5);
		};

		StaticText(win, Rect(900, 528, 58, 18)).string = "SC SRC";
		scCutSourceMenu = PopUpMenu(win, Rect(960, 524, 110, 24));
		~scCutSourceMenu = scCutSourceMenu;
		scCutSourceMenu.items = ["material", "live", "blend"];
		scCutSourceMenu.value = 0;
		scCutSourceMenu.action = {
			~setScCutSource.([\material, \live, \blend][scCutSourceMenu.value]);
		};

		StaticText(win, Rect(900, 560, 58, 18)).string = "WAIT";
		bedWaitMinBox = NumberBox(win, Rect(960, 556, 50, 22));
		bedWaitMinBox.value = ~bedMinWait;
		bedWaitMinBox.action = { ~bedMinWait = bedWaitMinBox.value.max(0.25) };
		bedWaitMaxBox = NumberBox(win, Rect(1020, 556, 50, 22));
		bedWaitMaxBox.value = ~bedMaxWait;
		bedWaitMaxBox.action = { ~bedMaxWait = bedWaitMaxBox.value.max(0.25) };

		StaticText(win, Rect(900, 588, 58, 18)).string = "DUR";
		bedDurMinBox = NumberBox(win, Rect(960, 584, 50, 22));
		bedDurMinBox.value = ~bedMinDur;
		bedDurMinBox.action = { ~bedMinDur = bedDurMinBox.value.max(1.0) };
		bedDurMaxBox = NumberBox(win, Rect(1020, 584, 50, 22));
		bedDurMaxBox.value = ~bedMaxDur;
		bedDurMaxBox.action = { ~bedMaxDur = bedDurMaxBox.value.max(1.0) };

		Button(win, Rect(900, 620, 170, 32))
			.states_([["RELOAD TAPES"]])
			.action_({
				~sampleRoot = pathField.string;
				~loadSamplesAsync.();
			});

		Button(win, Rect(900, 660, 170, 32))
			.states_([["REFRESH CHUNKS"]])
			.action_({
				~chunkSourceRoot = pathField.string;
				~refreshChunksOnce.();
			});

		Button(win, Rect(900, 700, 170, 32))
			.states_([["STOP LOAD"]])
			.action_({
				~stopLoader.();
				~setStatus.("Loader stopped");
				~setCount.("Loaded: " ++ ~loadedCount ++ " / " ++ ~loadTargetCount ++ " (stopped)");
			});

		Button(win, Rect(900, 740, 170, 32))
			.states_([["FREE BUFFERS"]])
			.action_({
				~stopLoader.();
				~stopAll.();
				~freeBuffers.();
				~setStatus.("Buffers freed");
			});

		Button(win, Rect(900, 780, 170, 32))
			.states_([["KILL ALL"]])
			.action_({
				~stopLoader.();
				~stopAll.();
				~resetButtons.();
			});

		StaticText(win, Rect(20, 828, 1080, 20)).string =
			"Workflow: LOAD -> wait for READY -> set density / voices -> shape tape strip -> start layers or SCENE AUTO -> REC IN / REC OUT";

		StaticText(win, Rect(20, 856, 1080, 20)).string =
			"SC SRC drives SC CUTS and BED LOOP: material, live, or blend; SC INPUT toggles live capture over loaded material.";

		win.onClose = {
			~stopLoader.();
			~stopAll.();
			~freeBuffers.();

			if(~master.notNil)      { try { ~master.free } };
			if(~mixBus.notNil)      { try { ~mixBus.free } };
			if(~liveGroup.notNil)   { try { ~liveGroup.free } };
			if(~liveCutGroup.notNil) { try { ~liveCutGroup.free } };
			if(~bedGroup.notNil)    { try { ~bedGroup.free } };
			if(~cutsGroup.notNil)   { try { ~cutsGroup.free } };
			if(~loopsGroup.notNil)  { try { ~loopsGroup.free } };
			if(~ghostsGroup.notNil) { try { ~ghostsGroup.free } };
			if(~cloudGroup.notNil)  { try { ~cloudGroup.free } };
		};

		~setStatus.("GUI ready");
	};

	{
		makeUI.();
	}.defer;

});
)
