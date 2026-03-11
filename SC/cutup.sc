/*

TRANSMISSIONS TAPE-DECK GUI
WAV-ONLY / NON-RECURSIVE / DIRECT BUFFER LOAD

FIXES
- scans only *.wav in the selected folder
- no recursive folder walking
- direct Buffer.read for every matched file
- READY only after buffers are actually added
- SCENE AUTO checks real buffer count

USAGE
1. Boot server
2. Run the whole block
3. Point TAPE ROOT to a folder containing only .wav files
4. Press LOAD
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

	~buffers = List.new;
	~sampleMeta = List.new;
	~allPaths = List.new;
	~active = IdentityDictionary.new;

	~mixBus = nil;
	~master = nil;

	~cutsGroup = nil;
	~loopsGroup = nil;
	~ghostsGroup = nil;
	~cloudGroup = nil;

	~loaderRoutine = nil;
	~isLoading = false;
	~loadTargetCount = 0;
	~loadedCount = 0;

	~density = \medium;

	~masterAmp = 0.95;
	~driveAmt = 1.2;
	~wowAmt = 0.01;
	~flutterAmt = 0.006;
	~dropoutAmt = 0.03;
	~dirtAmt = 0.05;
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
		}.defer;
	};

	// ============================================================
	// GROUPS / CLEANUP
	// ============================================================

	~makeGroups = {
		if(~cutsGroup.notNil)   { try { ~cutsGroup.free } };
		if(~loopsGroup.notNil)  { try { ~loopsGroup.free } };
		if(~ghostsGroup.notNil) { try { ~ghostsGroup.free } };
		if(~cloudGroup.notNil)  { try { ~cloudGroup.free } };

		~cutsGroup   = Group.head(s);
		~loopsGroup  = Group.after(~cutsGroup);
		~ghostsGroup = Group.after(~loopsGroup);
		~cloudGroup  = Group.after(~ghostsGroup);
	};

	~hardStopCuts = {
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

	~stopSceneOnly = {
		~active.removeAt(\scene);
		{ if(~sceneBtn.notNil) { ~sceneBtn.value = 0 } }.defer;
	};

	~stopLayers = {
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
		~isLoading = false;
		~active.removeAt(\loader);
		~loaderRoutine = nil;
	};

	~freeBuffers = {
		~buffers.do { |b| try { b.free } };
		~buffers.clear;
		~sampleMeta.clear;
		~allPaths.clear;
		~loadedCount = 0;
		~loadTargetCount = 0;
		~setCount.("Loaded: 0");
		~setProgress.(0);
	};

	// ============================================================
	// WAV ONLY / NON-RECURSIVE SCAN
	// ============================================================

	~scanWavFolder = { |root|
		var files, normalizedRoot, pattern;

		normalizedRoot = root.asString.standardizePath;

		if(File.exists(normalizedRoot).not) {
			("Folder not found: " ++ normalizedRoot).warn;
			~setStatus.("Folder not found");
			~setCount.("Loaded: 0");
			~setProgress.(0);
			^List.new;
		};

		~setStatus.("Scanning .wav folder...");
		~setCount.("Looking for wav files...");
		~setProgress.(0.1);

		pattern = normalizedRoot +/+ "*.wav";
		("SCAN PATTERN: " ++ pattern).postln;

		files = pattern.pathMatch;

		if(files.isNil) { files = [] };

		files = files.asArray.sort;

		("WAV FILES FOUND: " ++ files.size).postln;
		files.do { |p| p.postln };

		if(files.isEmpty) {
			~setStatus.("No .wav files found");
			~setCount.("Loaded: 0");
			~setProgress.(0);
			^List.new;
		};

		~setStatus.("Scan complete");
		~setCount.("Found: " ++ files.size);
		~setProgress.(0.2);

		^files.as(List);
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

	~loadOnePath = { |path|
		Buffer.read(s, path, action: { |b|
			var dur;

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

				~loadedCount = ~loadedCount + 1;

				("BUFFER OK: " ++ PathName.new(path).fileName ++ " | total=" ++ ~loadedCount).postln;

				~setCount.("Loaded: " ++ ~loadedCount ++ " / " ++ ~loadTargetCount);
				~setStatus.("Loaded: " ++ PathName.new(path).fileName);

				if(~loadTargetCount > 0) {
					~setProgress.(0.25 + ((~loadedCount / ~loadTargetCount) * 0.75));
				};

				if(~loadedCount >= ~loadTargetCount) {
					~isLoading = false;
					~active.removeAt(\loader);
					~loaderRoutine = nil;
					~setStatus.("READY");
					~setCount.("Loaded: " ++ ~loadedCount);
					~setProgress.(1.0);
				};
			} {
				("FAILED TO LOAD BUFFER: " ++ path).warn;
				if(b.notNil) { try { b.free } };
			};
		});
	};

	~loadSamplesAsync = {
		var files;

		~sampleRoot = ~sampleRoot.asString.standardizePath;
		("LOAD -> " ++ ~sampleRoot).postln;

		~stopLoader.();
		~stopAll.();
		~freeBuffers.();

		files = ~scanWavFolder.(~sampleRoot);

		if(files.isEmpty) {
			~setStatus.("No .wav files found");
			~setCount.("Loaded: 0");
			~setProgress.(0);
			^nil;
		};

		~allPaths = files;
		~loadTargetCount = ~allPaths.size;
		~loadedCount = 0;
		~isLoading = true;

		~setStatus.("Loading wav files...");
		~setCount.("Loaded: 0 / " ++ ~loadTargetCount);
		~setProgress.(0.25);

		~allPaths.do { |path|
			("QUEUE LOAD: " ++ path).postln;
			~loadOnePath.(path);
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
		|in=0, out=0, amp=1.0, drive=1.2, wow=0.01, flutter=0.006, dropout=0.03, dirt=0.05, dub=0.08, lp=11000, hp=45|
		var dry, sig, wowMod, flutterMod, hiss, crackle, gate, delA, delB;

		dry = In.ar(in, 2);
		wowMod = SinOsc.kr(0.18, 0, wow, 1.0);
		flutterMod = SinOsc.kr(5.8, 0, flutter, 1.0);
		sig = DelayC.ar(dry, 0.05, (((wowMod * flutterMod) - 1).abs * 0.01));
		sig = tanh(sig * drive);

		hiss = HPF.ar(WhiteNoise.ar(dirt * 0.05), 5000);
		crackle = Decay2.ar(Dust.ar((dirt * 100).max(1)), 0.0008, 0.01) * WhiteNoise.ar(dirt * 0.12);

		delA = DelayC.ar(sig, 0.3, 0.11);
		delB = DelayC.ar(sig, 0.4, 0.22);

		gate = Lag.kr((LFNoise1.kr(4.5) > (-1 + (dropout * 2))).max(0), 0.02);

		sig = sig + (delA * dub) + (delB * (dub * 0.7));
		sig = sig + hiss + Pan2.ar(crackle, LFNoise1.kr(1.2));
		sig = sig * gate;
		sig = HPF.ar(sig, hp);
		sig = LPF.ar(sig, lp);
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
				\dub, ~dubAmt,
				\lp, ~lpFreq,
				\hp, ~hpFreq
			);
		};
	};

	// ============================================================
	// ENGINES
	// ============================================================

	~startCuts = {
		if(~buffers.isEmpty) {
			~setStatus.("No buffers loaded — press LOAD first");
			^nil;
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
	};

	~startLoops = { |numVoices = 4|
		var voices;

		if(~buffers.isEmpty) {
			~setStatus.("No buffers loaded — press LOAD first");
			^nil;
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
	};

	~startGhosts = { |numVoices = 3|
		var voices;

		if(~buffers.isEmpty) {
			~setStatus.("No buffers loaded — press LOAD first");
			^nil;
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
	};

	~startCloud = { |numVoices = 4|
		var voices;

		if(~buffers.isEmpty) {
			~setStatus.("No buffers loaded — press LOAD first");
			^nil;
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
	};

	~startScene = {
		var sceneRoutine;

		("SCENE CHECK -> buffers: " ++ ~buffers.size ++ " sampleMeta: " ++ ~sampleMeta.size).postln;

		if(~buffers.isEmpty) {
			~setStatus.("No buffers loaded — press LOAD first");
			^nil;
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
	};

	~stab = {
		var idx, meta, dur, fragDur, start;

		if(~buffers.isEmpty) {
			~setStatus.("No buffers loaded — press LOAD first");
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

		if(~buffers.isEmpty) {
			~setStatus.("No buffers loaded — press LOAD first");
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

	~recordStart = {
		s.prepareForRecord(~recordPath);
		s.record;
		~setStatus.("Recording...");
	};

	~recordStop = {
		s.stopRecording;
		~setStatus.("Recording stopped");
	};

	// ============================================================
	// UI
	// ============================================================

	makeUI = {
		var win, pathField, densityMenu;
		var cloudVoicesBox, loopVoicesBox, ghostVoicesBox;
		var masterSlider, driveSlider, wowSlider, flutterSlider, dropoutSlider, dirtSlider, dubSlider, lpSlider, hpSlider;
		var cutsSlider, loopsSlider, ghostsSlider, cloudSlider;
		var cutsMuteBtn, loopsMuteBtn, ghostsMuteBtn, cloudMuteBtn;

		win = Window("TRANSMISSIONS TAPE-DECK — WAV DIRECT LOAD", Rect(60, 60, 1140, 680)).front;
		win.alwaysOnTop = true;

		StaticText(win, Rect(20, 15, 100, 20)).string = "TAPE ROOT";

		pathField = TextField(win, Rect(110, 12, 740, 24));
		pathField.string = ~sampleRoot;

		Button(win, Rect(860, 12, 80, 24))
			.states_([["SET"]])
			.action_({
				~sampleRoot = pathField.string;
				~setStatus.("Path updated");
			});

		Button(win, Rect(950, 12, 70, 24))
			.states_([["LOAD"]])
			.action_({
				~sampleRoot = pathField.string;
				~loadSamplesAsync.();
			});

		Button(win, Rect(1030, 12, 80, 24))
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

		Button(win, Rect(980, 104, 55, 28))
			.states_([["STAB"]])
			.action_({ ~stab.() });

		Button(win, Rect(1045, 104, 55, 28))
			.states_([["PHRS"]])
			.action_({ ~phraseShot.() });

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

		StaticText(win, Rect(350, 175, 60, 18)).string = "DUB";
		dubSlider = Slider(win, Rect(355, 200, 28, 210));
		dubSlider.orientation = \vertical;
		dubSlider.value = ~dubAmt.linlin(0, 0.8, 0, 1);
		dubSlider.action = {
			~dubAmt = dubSlider.value.linlin(0, 1, 0.0, 0.8);
			~updateMasterFX.();
		};

		StaticText(win, Rect(405, 175, 60, 18)).string = "LP";
		lpSlider = Slider(win, Rect(410, 200, 28, 210));
		lpSlider.orientation = \vertical;
		lpSlider.value = ~lpFreq.linlin(1500, 14000, 0, 1);
		lpSlider.action = {
			~lpFreq = lpSlider.value.linlin(0, 1, 1500, 14000);
			~updateMasterFX.();
		};

		StaticText(win, Rect(460, 175, 60, 18)).string = "HP";
		hpSlider = Slider(win, Rect(465, 200, 28, 210));
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

		StaticText(win, Rect(900, 175, 180, 18)).string = "TRANSPORT / SCENE";

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

		Button(win, Rect(900, 260, 170, 36))
			.states_([["RELOAD TAPES"]])
			.action_({
				~sampleRoot = pathField.string;
				~loadSamplesAsync.();
			});

		Button(win, Rect(900, 310, 170, 36))
			.states_([["STOP LOAD"]])
			.action_({
				~stopLoader.();
				~setStatus.("Loader stopped");
			});

		Button(win, Rect(900, 360, 170, 36))
			.states_([["FREE BUFFERS"]])
			.action_({
				~stopAll.();
				~freeBuffers.();
				~setStatus.("Buffers freed");
			});

		Button(win, Rect(900, 410, 170, 36))
			.states_([["KILL ALL"]])
			.action_({
				~stopLoader.();
				~stopAll.();
				~resetButtons.();
			});

		StaticText(win, Rect(20, 560, 1080, 20)).string =
			"Workflow: LOAD -> wait for READY -> set density / voices -> shape tape strip -> start layers or SCENE AUTO -> REC IN / REC OUT";

		StaticText(win, Rect(20, 588, 1080, 20)).string =
			"WAV-only, non-recursive, direct buffer load. Watch post window for BUFFER OK.";

		win.onClose = {
			~stopLoader.();
			~stopAll.();
			~freeBuffers.();

			if(~master.notNil)      { try { ~master.free } };
			if(~mixBus.notNil)      { try { ~mixBus.free } };
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
