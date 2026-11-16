import { createHash, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import { closeSync, mkdtempSync, openSync, readFileSync, rmdirSync, unlinkSync, writeSync } from "node:fs";
import { mkdir, mkdtemp, open, readFile, rename, rm, writeFile } from "node:fs/promises";
import { dirname, join, posix } from "node:path";
import { Duplex, PassThrough, Writable } from "node:stream";
import { homedir, tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { EventEmitter } from "node:events";
import { FS_ERROR_CODES } from "@deepseek-ai/dsh-fs";
import { pipeline } from "node:stream/promises";
//#region src/protocol.ts
/** Plugin version this client build expects; the server package must match exactly. */
const REMOTE_WORKSPACE_PLUGIN_VERSION = "0.1.0-alpha.7";
/** Base path of the P1 plugin API on the profile server. */
const RW_ROUTE_PREFIX = "/dsh-remote-workspace-server";
/** Exact P1 routes (protocol §4.1 / §6 / §7 / §9). */
const RW_PATHS = {
	handshake: `${RW_ROUTE_PREFIX}/api/handshake`,
	rpc: `${RW_ROUTE_PREFIX}/api/rpc`,
	events: `${RW_ROUTE_PREFIX}/api/events`,
	mux: `${RW_ROUTE_PREFIX}/api/mux`,
	bootstrap: `${RW_ROUTE_PREFIX}/embed/bootstrap.js`,
	embedCss: `${RW_ROUTE_PREFIX}/embed/embed.css`,
	sessionEvents: `${RW_ROUTE_PREFIX}/api/session/events`,
	clientTargets: `${RW_ROUTE_PREFIX}/api/client/targets`
};
/** P1 request headers identifying the lease owner. */
const RW_HEADERS = {
	client: "x-dsh-rw-client",
	instance: "x-dsh-rw-instance",
	protocol: "x-dsh-rw-protocol"
};
/** P1 RPC method names (protocol §4.2 + §16). Frozen by protocol §16. */
const RW_METHODS = {
	heartbeat: "profile/heartbeat",
	release: "profile/release",
	status: "profile/status",
	cancelAll: "profile/cancel-all",
	workspaceList: "workspace/list",
	workspaceCreate: "workspace/create",
	workspaceRename: "workspace/rename",
	workspaceDelete: "workspace/delete",
	workspaceReorder: "workspace/reorder",
	sessionList: "session/list",
	sessionCreate: "session/create",
	sessionPrepareServer: "session/prepare-server",
	sessionRename: "session/rename",
	sessionFork: "session/fork",
	sessionPin: "session/pin",
	sessionArchive: "session/archive",
	sessionCancel: "session/cancel",
	sessionReorder: "session/reorder",
	persistList: "persist/list",
	persistStat: "persist/stat",
	persistCreate: "persist/create",
	persistOpen: "persist/open",
	persistRead: "persist/read",
	persistAppend: "persist/append",
	persistFlush: "persist/flush",
	persistClose: "persist/close",
	persistLocate: "persist/locate",
	executorGet: "executor/get",
	executorAssign: "executor/assign",
	executorPlanSwitch: "executor/plan-switch",
	executorSwitch: "executor/switch",
	executorRelease: "executor/release",
	registryStatus: "registry/status",
	registryRefresh: "registry/refresh",
	fsList: "fs/list",
	sessionTimeline: "session/timeline",
	sessionPrompt: "session/prompt",
	sessionCommands: "session/commands",
	sessionCommand: "session/command",
	sessionModelCatalog: "session/modelCatalog",
	sessionSelectModel: "session/selectModel",
	sessionApproval: "session/approval",
	sessionStatus: "session/status",
	clientTargets: "client/targets"
};
//#endregion
//#region src/server-plugin/onboarding.ts
/**
* Click the first enabled 继续/Continue button inside a dialog/overlay.
* @param root - document-like root queried for dialog containers.
* @returns true when one button was clicked; false when no eligible button exists.
*/
function dismissOnboarding(root) {
	const dialogs = root.querySelectorAll("[role=\"dialog\"], dialog, [aria-modal=\"true\"]");
	for (let index = 0; index < dialogs.length; index++) {
		const dialog = dialogs[index];
		if (dialog === void 0) continue;
		const buttons = dialog.querySelectorAll("button, [role=\"button\"]");
		for (let buttonIndex = 0; buttonIndex < buttons.length; buttonIndex++) {
			const button = buttons[buttonIndex];
			if (button === void 0 || button.disabled === true) continue;
			const label = (button.textContent ?? "").replace(/\s+/gu, " ").trim().toLocaleLowerCase();
			if (label !== "继续" && label !== "continue") continue;
			if (typeof button.click !== "function") continue;
			button.click();
			return true;
		}
	}
	return false;
}
/** Outer bootstrap with the tested dismissal helper inlined for the iframe realm. */
const BOOTSTRAP_SCRIPT = String.raw`(function () {
  'use strict';
  var config = window.__DSH_RW_EMBED__ || {};
  var prefix = typeof config.storagePrefix === 'string' ? config.storagePrefix : 'dshrw:';
  var marker = '__dshRwShimmed__';
  var __dshRwOnboarding__ = __DISMISS_ONBOARDING__;
  var parentOrigin = typeof config.parentOrigin === 'string' ? config.parentOrigin : window.location.origin;
  if (!window[marker]) {
    try {
      document.documentElement.setAttribute('data-dsh-rw-embed', '');
      // Full mode keeps the official sidebar/rightbar/bottom slot visible (the
      // v3 client renders the complete official shell in a full-screen iframe).
      // The client sends dshRwFull=1; full=1 is the documented/plan spelling.
      var initialParams = new URLSearchParams(window.location.search || '');
      var fullParam = initialParams.get('dshRwFull');
      var fullFallback = initialParams.get('full');
      if (fullParam === '1' || fullFallback === '1') document.documentElement.setAttribute('data-dsh-rw-full', '');
    } catch (e) {}
    if (config.embedCssPath) {
      var link = document.createElement('link');
      link.rel = 'stylesheet';
      link.href = config.embedCssPath;
      (document.head || document.documentElement).appendChild(link);
    }
    function wrapStorage(name) {
      var storage = window[name];
      if (!storage) return;
      var proto = Object.getPrototypeOf(storage);
      var original = {
        getItem: proto.getItem,
        setItem: proto.setItem,
        removeItem: proto.removeItem,
        clear: proto.clear,
        key: proto.key
      };
      function keys() {
        var out = [];
        try {
          var total = 0;
          for (var i = 0; ; i++) {
            var k = original.key.call(storage, i);
            if (k === null || k === undefined) break;
            if (typeof k === 'string' && k.indexOf(prefix) === 0) out.push(k);
            total++;
            if (total > 100000) break;
          }
        } catch (e) {}
        return out;
      }
      storage.getItem = function (key) { return original.getItem.call(storage, prefix + String(key)); };
      storage.setItem = function (key, value) { return original.setItem.call(storage, prefix + String(key), value); };
      storage.removeItem = function (key) { return original.removeItem.call(storage, prefix + String(key)); };
      storage.clear = function () {
        keys().forEach(function (key) { original.removeItem.call(storage, key); });
      };
      storage.key = function (index) {
        var list = keys();
        var i = Number(index);
        if (!isFinite(i) || i < 0 || i >= list.length) return null;
        return list[i].slice(prefix.length);
      };
      try {
        Object.defineProperty(storage, 'length', { configurable: true, get: function () { return keys().length; } });
      } catch (e) {}
    }
    wrapStorage('localStorage');
    wrapStorage('sessionStorage');
    window[marker] = true;
  }
  try {
    var params = new URLSearchParams(window.location.search || '');
    var sessionId = params.get('dshRwSession');
    if (sessionId) {
      window.localStorage.setItem('dsh.sessions.current', JSON.stringify({ sessionId: sessionId }));
      // The official preview notice can cover the restored blank composer for
      // several seconds; dismiss only its exact 继续/Continue action, once.
      var onboardingTries = 0;
      var onboardingTimer = setInterval(function () {
        try { if (__dshRwOnboarding__(document)) { clearInterval(onboardingTimer); return; } } catch (e) {}
        if (++onboardingTries >= 100) clearInterval(onboardingTimer);
      }, 100);
    }
    var theme = params.get('dshRwTheme');
    if (theme === 'light' || theme === 'dark') document.documentElement.setAttribute('data-theme', theme);
    var lang = params.get('dshRwLang');
    if (lang) document.documentElement.setAttribute('lang', lang);
  } catch (e) {}
  window.addEventListener('message', function (event) {
    if (parentOrigin !== '*' && event.origin !== parentOrigin) return;
    var data = event.data;
    if (!data || data.v !== 1) return;
    if (data.type === 'dsh-rw/open-session' && typeof data.sessionId === 'string') {
      try { window.localStorage.setItem('dsh.sessions.current', JSON.stringify({ sessionId: data.sessionId })); } catch (e) {}
      window.location.reload();
    } else if (data.type === 'dsh-rw/reload') {
      window.location.reload();
    }
  });
  try {
    if (window.parent && window.parent !== window) window.parent.postMessage({ v: 1, type: 'dsh-rw/ready' }, parentOrigin);
  } catch (e) {}
})();`.replace("__DISMISS_ONBOARDING__", `(${dismissOnboarding.toString()})`);
/**
* Hide the duplicated official navigation and let the conversation main area fill the frame.
*
* Every rule is gated by `:not([data-dsh-rw-full])`: a full embed (v3 client:
* full-screen same-origin iframe of the official UI) keeps the complete official
* shell, so the sidebar, rightbar, bottom slot and root grid are left untouched.
*/
const EMBED_CSS = String.raw`html[data-dsh-rw-embed]:not([data-dsh-rw-full]) [data-slot="sidebar"],
html[data-dsh-rw-embed]:not([data-dsh-rw-full]) [data-slot="sidebar"] * { display: none !important; }
html[data-dsh-rw-embed]:not([data-dsh-rw-full]) [data-slot="rightbar"],
html[data-dsh-rw-embed]:not([data-dsh-rw-full]) [data-slot="rightbar"] * { display: none !important; }
html[data-dsh-rw-embed]:not([data-dsh-rw-full]) [data-slot="shell.bottom"] { display: none !important; }
html[data-dsh-rw-embed]:not([data-dsh-rw-full]) [data-slot="root"] > div > div { grid-template-columns: minmax(0, 1fr) !important; }`;
//#endregion
//#region src/server-plugin/errors.ts
/** Stable error object shape shared by the v1 server plugin (protocol §2.1). */
var PluginError = class extends Error {
	code;
	details;
	isDSHRemoteError = true;
	constructor(code, message, details, options) {
		super(message, options);
		this.code = code;
		this.details = details;
		this.name = "RemoteError";
	}
};
/** Scrub known secret shapes from diagnostics before they leave the process. */
function scrubSecrets(text) {
	return text.replace(/\b(Bearer\s+)[A-Za-z0-9._~+/=-]{8,}/gi, "$1[redacted]").replace(/\b(token|password|passphrase|secret|api[_-]?key)(\s*[=:]\s*)[^\s,;'"]+/gi, "$1$2[redacted]").replace(/\bsk-[A-Za-z0-9_-]{8,}/g, "[redacted]");
}
/** Convert unknown errors into a safe envelope error. */
function errorValue(error, fallbackCode = "internal") {
	const candidate = error;
	return {
		code: typeof candidate?.code === "string" ? candidate.code : fallbackCode,
		message: scrubSecrets(error instanceof Error ? error.message : String(error)),
		...candidate?.details === void 0 ? {} : { details: candidate.details }
	};
}
/**
* HTTP status for one stable P1 error code (protocol §12).
*
* Explicit table: an unmapped business code is a contract bug, not a silent
* 500. Keep this in sync with `RW_ERROR_CODES`; `internal` is the only 500
* fallback.
*/
function httpStatusFor(code) {
	switch (code) {
		case "bad-request":
		case "workspace-path-invalid":
		case "persist-read-only": return 400;
		case "unauthorized": return 401;
		case "forbidden":
		case "lease-mismatch": return 403;
		case "plugin-missing":
		case "workspace-unknown":
		case "session-not-found":
		case "target-not-connected": return 404;
		case "plugin-protocol-mismatch":
		case "plugin-version-mismatch":
		case "profile-mismatch":
		case "profile-foreign-client":
		case "profile-in-use":
		case "registry-stale":
		case "workspace-not-empty":
		case "session/writer-held":
		case "session/agent-busy":
		case "session/archive-activity":
		case "session/archived-pin":
		case "executor-held":
		case "executor-mode-conflict":
		case "executor-switch-confirm-required":
		case "executor-switch-busy":
		case "persist-create-exists":
		case "agent-unavailable": return 409;
		case "handle-not-found": return 410;
		case "upstream-timeout": return 502;
		case "tunnel-unavailable": return 503;
		case "timeout": return 504;
		default: return 500;
	}
}
/** Phases the browser knows how to render; anything else is not a connection state. */
const CLIENT_TARGET_PHASES = [
	"connecting",
	"ready",
	"degraded",
	"failed",
	"stopped",
	"disconnected"
];
/** The empty snapshot the route serves before the client publishes anything. */
function emptyClientTargets() {
	return {
		clientId: "",
		updatedAt: 0,
		targets: []
	};
}
/** Trimmed string within a byte-agnostic length budget, or undefined. */
function boundedString(value, max) {
	if (typeof value !== "string") return void 0;
	const trimmed = value.trim();
	if (trimmed === "" || trimmed.length > max) return void 0;
	return scrubSecrets(trimmed);
}
/** Integer inside an inclusive range, or undefined. */
function boundedInteger(value, min, max) {
	if (typeof value !== "number" || !Number.isSafeInteger(value)) return void 0;
	return value >= min && value <= max ? value : void 0;
}
/** One whitelisted row, or undefined when a required field is unusable. */
function normalizeRow(value) {
	if (value === null || typeof value !== "object" || Array.isArray(value)) return void 0;
	const row = value;
	const targetId = boundedString(row["targetId"], 200);
	if (targetId === void 0) return void 0;
	const kind = row["kind"] === "ssh" || row["kind"] === "wsl" ? row["kind"] : void 0;
	if (kind === void 0) return void 0;
	const phase = CLIENT_TARGET_PHASES.find((entry) => entry === row["phase"]);
	if (phase === void 0) return void 0;
	const label = boundedString(row["label"], 200) ?? targetId;
	const host = boundedString(row["host"], 255);
	const username = boundedString(row["username"], 128);
	const port = row["port"] === void 0 ? void 0 : boundedInteger(row["port"], 0, 65535);
	return {
		targetId,
		label,
		kind,
		...host === void 0 ? {} : { host },
		...port === void 0 ? {} : { port },
		...username === void 0 ? {} : { username },
		phase,
		...row["active"] === true ? { active: true } : {}
	};
}
/**
* Whitelist and bound one published snapshot.
*
* @param value - raw `client/targets` params.
* @returns the normalized snapshot (rows deduplicated by id, truncated to 64).
* @throws PluginError `bad-request` when the envelope itself is unusable.
*/
function normalizeClientTargets(value) {
	if (value === null || typeof value !== "object" || Array.isArray(value)) throw new PluginError("bad-request", "client/targets requires an object body");
	const input = value;
	const clientId = boundedString(input["clientId"], 200);
	if (clientId === void 0) throw new PluginError("bad-request", "client/targets requires a non-empty clientId");
	const updatedAt = boundedInteger(input["updatedAt"], 0, Number.MAX_SAFE_INTEGER);
	if (updatedAt === void 0) throw new PluginError("bad-request", "client/targets requires an integer updatedAt");
	if (!Array.isArray(input["targets"])) throw new PluginError("bad-request", "client/targets requires a targets array");
	const seen = /* @__PURE__ */ new Set();
	const targets = [];
	for (const raw of input["targets"]) {
		if (targets.length >= 64) break;
		const row = normalizeRow(raw);
		if (row === void 0 || seen.has(row.targetId)) continue;
		seen.add(row.targetId);
		targets.push(row);
	}
	const activeTargetId = boundedString(input["activeTargetId"], 200);
	return {
		clientId,
		updatedAt,
		...activeTargetId === void 0 ? {} : { activeTargetId },
		targets
	};
}
/**
* The server profile's in-memory copy of the client's connection list.
*
* `updatedAt` is monotonic: a snapshot that is not strictly newer than the
* retained one is dropped, so a delayed publish can never resurrect a stale
* list or an old `activeTargetId`.
*/
var ClientTargetsSnapshot = class {
	current;
	/** The snapshot the browser should render (empty before the first publish). */
	read() {
		return this.current ?? emptyClientTargets();
	}
	/** Retained stamp, for diagnostics and the dispatch reply. */
	get updatedAt() {
		return this.current?.updatedAt ?? 0;
	}
	/** Whether any publication has been retained (diagnostics/tests). */
	get present() {
		return this.current !== void 0;
	}
	/**
	* Validate, authenticate and retain one published snapshot.
	* @param value - raw `client/targets` params.
	* @param options - `leaseClientId`: the active lease owner, when known.
	* @returns whether the value won the `updatedAt` race, plus the retained state.
	* @throws PluginError `bad-request` when the envelope is unusable,
	*   `lease-mismatch` when it claims another client than the lease.
	*/
	accept(value, options = {}) {
		const next = normalizeClientTargets(value);
		this.assertLeaseOwner(next, options.leaseClientId);
		if (this.current !== void 0 && next.updatedAt <= this.current.updatedAt) return {
			accepted: false,
			updatedAt: this.current.updatedAt,
			count: this.current.targets.length
		};
		this.current = next;
		return {
			accepted: true,
			updatedAt: next.updatedAt,
			count: next.targets.length
		};
	}
	/**
	* Authenticate one publication against the active lease.
	*
	* The RPC already checked the `x-dsh-rw-*` headers; this guards the body from
	* claiming a different client than the lease owner (protocol §16.14, §2.2).
	* @param value - normalized snapshot.
	* @param leaseClientId - the active lease's client id, when known.
	* @throws PluginError `lease-mismatch`.
	*/
	assertLeaseOwner(value, leaseClientId) {
		if (leaseClientId === void 0 || leaseClientId === value.clientId) return;
		throw new PluginError("lease-mismatch", "the published clientId does not match the active profile lease", {
			leaseClientId,
			publishedClientId: value.clientId
		});
	}
	/** Drop the retained snapshot (lease release / explicit reset). */
	clear() {
		this.current = void 0;
	}
};
//#endregion
//#region src/server-plugin/lease.ts
/** Single-client profile lease (protocol §3.4). */
/** Stale lease TTL (heartbeat age). */
const LEASE_TTL_MS = 2e4;
/** Fresh claim decision; pure for tests. */
function decideLease(existing, request, now) {
	const stamp = new Date(now).toISOString();
	if (existing === void 0) return {
		kind: "claimed",
		staleTakeover: false,
		lease: {
			version: 1,
			profile: request.profile,
			clientId: request.clientId,
			instanceId: request.instanceId,
			pid: request.pid,
			port: request.port,
			startedAt: stamp,
			heartbeatAt: stamp,
			tokenFingerprint: request.tokenFingerprint
		}
	};
	if (existing.clientId !== request.clientId) return {
		kind: "foreign-client",
		ownerClientId: existing.clientId
	};
	const age = now - Date.parse(existing.heartbeatAt);
	if (existing.instanceId === request.instanceId) return {
		kind: "claimed",
		staleTakeover: false,
		lease: {
			...existing,
			profile: request.profile,
			pid: request.pid,
			port: request.port,
			heartbeatAt: stamp,
			tokenFingerprint: request.tokenFingerprint
		}
	};
	if (Number.isFinite(age) && age <= 2e4) return {
		kind: "in-use",
		owner: existing
	};
	return {
		kind: "claimed",
		staleTakeover: true,
		lease: {
			version: 1,
			profile: request.profile,
			clientId: request.clientId,
			instanceId: request.instanceId,
			pid: request.pid,
			port: request.port,
			startedAt: stamp,
			heartbeatAt: stamp,
			tokenFingerprint: request.tokenFingerprint
		}
	};
}
/** Read one lease document; absent/malformed = undefined. */
async function readLease(path) {
	try {
		const value = JSON.parse(await readFile(path, "utf8"));
		return value?.version === 1 && typeof value.profile === "string" && typeof value.clientId === "string" && typeof value.instanceId === "string" && typeof value.heartbeatAt === "string" ? value : void 0;
	} catch {
		return;
	}
}
/** Atomically write one lease document (0600). */
async function writeLease(path, lease) {
	await mkdir(dirname(path), {
		recursive: true,
		mode: 448
	});
	const temporary = `${path}.tmp-${process.pid}-${Math.random().toString(16).slice(2, 8)}`;
	await writeFile(temporary, `${JSON.stringify(lease)}\n`, { mode: 384 });
	await rename(temporary, path);
}
/** Remove the lease on explicit release. */
async function removeLease(path) {
	await rm(path, { force: true });
}
/** Owns the in-memory lease and its heartbeat. */
var ProfileLease = class {
	path;
	profile;
	heartbeatMs;
	lease;
	timer;
	serverId;
	constructor(path, profile, heartbeatMs = 5e3) {
		this.path = path;
		this.profile = profile;
		this.heartbeatMs = heartbeatMs;
		this.serverId = `${process.pid}`;
	}
	get current() {
		return this.lease;
	}
	get fingerprint() {
		return this.serverId;
	}
	/** Claim/refresh the lease for one handshake. */
	async claim(request) {
		const decision = decideLease(await readLease(this.path), {
			...request,
			profile: this.profile,
			pid: process.pid,
			port: request.port
		}, Date.now());
		if (decision.kind !== "claimed") return decision;
		this.lease = decision.lease;
		await writeLease(this.path, decision.lease);
		this.startHeartbeat();
		return decision;
	}
	/** Refresh heartbeat of the current lease. */
	async heartbeat() {
		if (this.lease === void 0) return {
			ok: false,
			expiresAt: (/* @__PURE__ */ new Date(0)).toISOString()
		};
		const now = (/* @__PURE__ */ new Date()).toISOString();
		this.lease = {
			...this.lease,
			heartbeatAt: now
		};
		await writeLease(this.path, this.lease).catch(() => {});
		return {
			ok: true,
			expiresAt: new Date(Date.parse(now) + LEASE_TTL_MS).toISOString()
		};
	}
	/** Whether a request header pair matches this instance. */
	matches(clientId, instanceId) {
		return this.lease !== void 0 && clientId === this.lease.clientId && instanceId === this.lease.instanceId;
	}
	/** Stop heartbeat and remove the lease (explicit release only). */
	async release() {
		this.stopHeartbeat();
		this.lease = void 0;
		await removeLease(this.path).catch(() => {});
	}
	/** Stop heartbeat without touching the file (process shutdown/effect cleanup). */
	dispose() {
		this.stopHeartbeat();
	}
	startHeartbeat() {
		this.stopHeartbeat();
		this.timer = setInterval(() => {
			this.heartbeat();
		}, this.heartbeatMs);
		this.timer.unref();
	}
	stopHeartbeat() {
		if (this.timer !== void 0) clearInterval(this.timer);
		this.timer = void 0;
	}
};
//#endregion
//#region src/server-plugin/file-lock.ts
/** Small cross-process writer lock with PID-stale takeover (withFileLock semantics). */
const DEFAULT_WAIT_MS = 2e3;
/** Whether a PID is still alive. */
function alive(pid) {
	try {
		process.kill(pid, 0);
		return true;
	} catch (error) {
		return error.code === "EPERM";
	}
}
/**
* Run one operation while holding `<path>.lock`.
* @param path - target file the lock protects.
* @param operation - critical section.
* @param waitMs - contention deadline.
*/
async function withFileLock(path, operation, waitMs = DEFAULT_WAIT_MS) {
	const lockPath = `${path}.lock`;
	await mkdir(dirname(lockPath), {
		recursive: true,
		mode: 448
	});
	const deadline = Date.now() + waitMs;
	for (;;) {
		let handle;
		try {
			handle = await open(lockPath, "wx", 384);
			await handle.writeFile(`${process.pid}\n`, "utf8");
			await handle.close();
			handle = void 0;
			break;
		} catch (error) {
			if (error.code !== "EEXIST") {
				await handle?.close().catch(() => {});
				throw error;
			}
			await handle?.close().catch(() => {});
			const owner = Number.parseInt(await readFile(lockPath, "utf8").catch(() => ""), 10);
			if (Number.isSafeInteger(owner) && owner > 0 && !alive(owner)) {
				await rm(lockPath, { force: true }).catch(() => {});
				continue;
			}
			if (Date.now() >= deadline) throw new Error(`timed out waiting for lock ${lockPath}`);
			await new Promise((resolve) => setTimeout(resolve, 25));
		}
	}
	try {
		return await operation();
	} finally {
		await rm(lockPath, { force: true }).catch(() => {});
	}
}
//#endregion
//#region src/server-plugin/registry-guard.ts
/**
* Cross-process workspace registry write guard (protocol §16.4).
*
* The official registry is an in-process snapshot over a whole-file JSON unit.
* This guard wraps the official prototypes so every write (plugin P1 and the
* profile's own official UI) takes a cross-process lock and compares the file
* revision before entering the official method.
*/
const WRAPPED = Symbol.for("dsh-rw.registry-wrapper");
let activeGuard;
/** Wrap one prototype method once, routing calls through the active guard. */
function wrapMethod(target, name) {
	const holder = target;
	const original = holder[name];
	if (typeof original !== "function") return false;
	if (original[WRAPPED] === true) return true;
	const wrapped = function(...args) {
		const guard = activeGuard;
		if (guard === void 0 || guard.disposed) return original.apply(this, args);
		const result = guard.write(() => original.apply(this, args));
		if (result !== null && typeof result === "object" && typeof result.then === "function") return result.then((value) => {
			guard.wrapEntityValue(value);
			return value;
		});
		guard.wrapEntityValue(result);
		return result;
	};
	Object.defineProperty(wrapped, WRAPPED, { value: true });
	holder[name] = wrapped;
	return true;
}
/** One guard per active workspace registry service. */
var RegistryGuard = class {
	ctx;
	registryFile;
	lockTarget;
	loadedRevision = "uninitialized";
	currentRevision = "uninitialized";
	epochValue = 0;
	changed = false;
	tail = Promise.resolve();
	wrapped = false;
	reloadAvailable = false;
	disposed = false;
	constructor(ctx, registryFile, lockTarget) {
		this.ctx = ctx;
		this.registryFile = registryFile;
		this.lockTarget = lockTarget;
	}
	async revision() {
		try {
			return createHash("sha256").update(await readFile(this.registryFile)).digest("hex");
		} catch (error) {
			if (error.code === "ENOENT") return "absent";
			throw error;
		}
	}
	/** Install prototype wrappers and seed the loaded revision. */
	async install() {
		const registry = this.ctx.get("workspaceRegistry");
		if (registry === void 0) return;
		activeGuard = this;
		this.loadedRevision = await this.revision();
		this.currentRevision = this.loadedRevision;
		const registryProto = Object.getPrototypeOf(registry);
		const writes = [
			"create",
			"delete",
			"insertBefore",
			"initializeDefault",
			"pinSession",
			"unpinSession",
			"archiveSession",
			"unarchiveSession"
		];
		let wrapped = 0;
		for (const method of writes) if (wrapMethod(registryProto, method)) wrapped += 1;
		const first = registry.list()[0];
		if (first === void 0 && registry instanceof Object) for (const method of [
			"list",
			"get",
			"create"
		]) {
			const proto = Object.getPrototypeOf(registry);
			const original = proto[method];
			if (typeof original !== "function" || original[WRAPPED] === true) continue;
			const wrappedRead = function(...args) {
				const value = original.apply(this, args);
				const guard = activeGuard;
				if (guard !== void 0) {
					if (Array.isArray(value)) {
						const item = value[0];
						if (item !== void 0) guard.wrapEntity(item);
					} else if (value instanceof Promise) value.then((item) => {
						if (item !== void 0) guard.wrapEntity(item);
					}).catch(() => {});
					else if (value !== void 0 && value !== null) guard.wrapEntity(value);
				}
				return value;
			};
			Object.defineProperty(wrappedRead, WRAPPED, { value: true });
			proto[method] = wrappedRead;
		}
		else if (first !== void 0) this.wrapEntity(first);
		this.reloadAvailable = this.findWorkspaceEntry() !== void 0;
		this.wrapped = wrapped > 0;
	}
	/** Wrap the prototype of a created/listed entity (idempotent). */
	wrapEntityValue(value) {
		if (value === null || typeof value !== "object") return;
		const candidate = value;
		if (typeof candidate.id !== "string" || typeof candidate.setTitle !== "function") return;
		this.wrapEntity(candidate);
	}
	/** Wrap one entity's prototype (once). */
	wrapEntity(entity) {
		const proto = Object.getPrototypeOf(entity);
		for (const method of [
			"setTitle",
			"attachSession",
			"detachSession",
			"insertSessionBefore"
		]) wrapMethod(proto, method);
	}
	/** Serialize a write through the cross-process lock and revision check. */
	write(operation) {
		return this.serial(async () => await withFileLock(this.lockTarget, async () => {
			if (this.loadedRevision === "uninitialized") this.loadedRevision = await this.revision();
			const current = await this.revision();
			this.currentRevision = current;
			if (current !== this.loadedRevision) {
				this.changed = true;
				throw new PluginError("registry-stale", "the server workspace registry changed since this profile loaded it; refresh/reconnect before writing", {
					loadedRevision: this.loadedRevision,
					currentRevision: current
				});
			}
			const result = await operation();
			this.loadedRevision = await this.revision();
			this.currentRevision = this.loadedRevision;
			this.changed = false;
			return result;
		}));
	}
	/** Probe the file revision without writing (read-only stale banner). */
	async probe() {
		this.currentRevision = await this.revision();
		this.changed = this.currentRevision !== this.loadedRevision;
	}
	/** Reload the official workspace row through the loader's disable/enable seam. */
	async refresh() {
		return await this.serial(async () => {
			const entry = this.findWorkspaceEntry();
			if (entry === void 0) throw new PluginError("registry-reload-failed", "cannot find the official workspace loader entry; restart the profile server instead");
			await withFileLock(this.lockTarget, async () => {
				await entry.update({ disabled: true });
				await entry.update({ disabled: false });
				await this.ctx.get("loader")?.await();
				this.loadedRevision = await this.revision();
				this.currentRevision = this.loadedRevision;
				this.epochValue += 1;
				this.changed = false;
				await this.install();
			});
			return {
				revision: this.loadedRevision,
				serviceReplaced: true
			};
		});
	}
	/** Read-only status for handshake/RPC responses. */
	status() {
		return {
			revision: this.currentRevision,
			loadedRevision: this.loadedRevision,
			epoch: String(this.epochValue),
			stale: this.changed,
			capability: this.wrapped ? "prototype" : "none",
			reload: this.reloadAvailable ? "loader-disable-enable" : "none"
		};
	}
	/** Stop routing new calls through this guard (prototype wrappers keep the guard object). */
	dispose() {
		this.disposed = true;
		if (activeGuard === this) activeGuard = void 0;
	}
	findWorkspaceEntry() {
		const loader = this.ctx.get("loader");
		if (loader === void 0) return void 0;
		const entries = [...loader.entries()].filter((entry) => entry.options.name === "@deepseek-ai/dsh-workspace");
		return entries.length === 1 ? entries[0] : void 0;
	}
	serial(operation) {
		const run = this.tail.then(operation, operation);
		this.tail = run.then(() => void 0, () => void 0);
		return run;
	}
};
/** Hard ceiling: refuse to decode above this size instead of risking an OOM. */
const EVENT_COUNT_DECODE_MAX_BYTES = 134217728;
/** Bound the per-artifact memo table. */
const CACHE_LIMIT = 512;
/** Bounded `(sessionId, sizeBytes)` → `{eventCount, foldedFacts}` memo. */
var SessionLogCache = class {
	entries = /* @__PURE__ */ new Map();
	warned = /* @__PURE__ */ new Set();
	key(sessionId, sizeBytes) {
		return `${sessionId}\0${sizeBytes}`;
	}
	/** Read the entry for one artifact identity. */
	get(sessionId, sizeBytes) {
		return this.entries.get(this.key(sessionId, sizeBytes));
	}
	/**
	* Apply the size policy before one decode: warn once above the soft limit,
	* refuse above the hard ceiling.
	*/
	allowDecode(sessionId, sizeBytes) {
		if (sizeBytes > 134217728) throw new PluginError("internal", `session "${sessionId}" log is ${sizeBytes} bytes; refusing to decode its facts above the ${EVENT_COUNT_DECODE_MAX_BYTES} byte safety ceiling`, {
			sessionId,
			sizeBytes,
			ceilingBytes: EVENT_COUNT_DECODE_MAX_BYTES
		});
		if (sizeBytes <= 8388608) return;
		const key = this.key(sessionId, sizeBytes);
		if (this.warned.has(key)) return;
		this.warned.add(key);
		console.error(`[dsh-remote-workspace-server] session "${sessionId}" log is ${sizeBytes} bytes; decoding it once for exact session facts (subsequent opens reuse this cache and the official revision memo)`);
	}
	/** Store facts decoded by a title fold (and the event count it observed). */
	rememberFacts(sessionId, sizeBytes, facts, eventCount) {
		this.update(sessionId, sizeBytes, {
			facts,
			eventCount
		});
	}
	/** Store both values decoded for an event count. */
	rememberCount(sessionId, sizeBytes, eventCount, facts) {
		this.update(sessionId, sizeBytes, {
			eventCount,
			facts
		});
	}
	/** Drop every entry of one session (session event/disposal invalidation). */
	invalidate(sessionId) {
		const prefix = `${sessionId}\0`;
		for (const key of [...this.entries.keys()]) if (key.startsWith(prefix)) this.entries.delete(key);
		for (const key of [...this.warned]) if (key.startsWith(prefix)) this.warned.delete(key);
	}
	update(sessionId, sizeBytes, fields) {
		const key = this.key(sessionId, sizeBytes);
		const existing = this.entries.get(key);
		this.entries.delete(key);
		this.entries.set(key, {
			sizeBytes,
			...existing?.eventCount === void 0 && fields.eventCount === void 0 ? {} : { eventCount: fields.eventCount ?? existing?.eventCount },
			...existing?.facts === void 0 && fields.facts === void 0 ? {} : { facts: fields.facts ?? existing?.facts }
		});
		if (this.entries.size > CACHE_LIMIT) {
			const oldest = this.entries.keys().next().value;
			if (oldest !== void 0) this.entries.delete(oldest);
		}
	}
};
//#endregion
//#region src/server-plugin/title-fold.ts
/** Fold display facts from one complete event log. */
function foldFacts(events) {
	let title;
	let blank = true;
	let updatedAt;
	for (const raw of events) {
		const event = raw;
		if (typeof event.time === "number" && (updatedAt === void 0 || event.time > updatedAt)) updatedAt = event.time;
		if (event.type === "session/title") {
			const value = event.data?.title;
			if (typeof value === "string") title = value;
		} else if (event.type === "turn/start") blank = false;
	}
	return {
		...title === void 0 || title === "" ? {} : { title },
		titleSource: title === void 0 || title === "" ? "fallback" : "log",
		blank,
		...updatedAt === void 0 ? {} : { updatedAt }
	};
}
/**
* Memoized log folder: one read per session per artifact size, four in flight.
* Read handles never take the write lease, so another process may keep writing.
* Facts and the event count share `SessionLogCache` with `persist/open`.
*/
var TitleFold = class {
	ctx;
	cache;
	active = 0;
	waiting = [];
	constructor(ctx, cache = new SessionLogCache()) {
		this.ctx = ctx;
		this.cache = cache;
	}
	/** Fold one session, reusing the shared size-keyed result. */
	async facts(sessionId, sizeBytes) {
		if (sizeBytes !== void 0) {
			const cached = this.cache.get(sessionId, sizeBytes);
			if (cached?.facts !== void 0) return cached.facts;
			try {
				this.cache.allowDecode(sessionId, sizeBytes);
			} catch (error) {
				console.error(`[dsh-remote-workspace-server] title fold for "${sessionId}" refused: ${error instanceof Error ? error.message : String(error)}`);
				return;
			}
		}
		const persistence = this.ctx.get("sessionPersistence");
		if (persistence === void 0) return void 0;
		await this.acquire();
		try {
			const handle = await persistence.open(sessionId, "read");
			try {
				const { events } = await handle.read(0);
				const facts = foldFacts(events);
				if (sizeBytes !== void 0) this.cache.rememberFacts(sessionId, sizeBytes, facts, events.length);
				return facts;
			} finally {
				await handle.close().catch(() => {});
			}
		} catch (error) {
			console.error(`[dsh-remote-workspace-server] title fold for "${sessionId}" failed: ${error instanceof Error ? error.message : String(error)}`);
			return;
		} finally {
			this.release();
		}
	}
	/** Drop one cached fact (session event/disposal invalidation). */
	invalidate(sessionId) {
		this.cache.invalidate(sessionId);
	}
	async acquire() {
		if (this.active < 4) {
			this.active += 1;
			return;
		}
		await new Promise((resolve) => {
			this.waiting.push(resolve);
		});
	}
	release() {
		const next = this.waiting.shift();
		if (next !== void 0) next();
		else this.active -= 1;
	}
};
/**
* Resolve a session row's blank flag across live metadata, cold summary and the
* JSONL fold.
*
* The official cold summary defaults `blank` to false when no projection cache
* row exists (`summarizeCold`), so that value is not evidence for a session in
* another process. Live summaries carry real projection metadata; cold rows
* rely on the folded `turn/start` check.
* @param metadataBlank - official sessionListMetadata.blank, when present.
* @param live - whether the summary came from a live agent.
* @param summaryBlank - the official summary's blank hint.
* @param foldedBlank - the JSONL fold's blank verdict, when decoded.
* @returns the row's blank flag.
*/
function resolveBlank(metadataBlank, live, summaryBlank, foldedBlank) {
	if (metadataBlank !== void 0) return metadataBlank;
	if (live && summaryBlank !== void 0) return summaryBlank;
	return foldedBlank ?? summaryBlank ?? true;
}
//#endregion
//#region src/server-plugin/managed.ts
/** Owns the managed AgentHandles of this profile process. */
var ManagedAgents = class {
	ctx;
	onSwitch;
	options;
	handles = /* @__PURE__ */ new Map();
	preparing = /* @__PURE__ */ new Set();
	constructor(ctx, onSwitch, options = {}) {
		this.ctx = ctx;
		this.onSwitch = onSwitch;
		this.options = options;
	}
	/** Agent-scoped approval registration; runs before the preset setup. */
	setupApproval(presetMount) {
		return async (agentCtx) => {
			this.options.approvals?.attachAgent(agentCtx, this.options.approvalPreempt === true);
			await presetMount?.(agentCtx);
		};
	}
	/** Ensure a live agent whose write lease is owned by this plugin. */
	async prepare(sessionId) {
		if (sessionId === "") throw new PluginError("bad-request", "session/prepare-server requires sessionId");
		const agents = this.ctx.get("agents");
		if (agents === void 0) throw new PluginError("internal", "the profile has no agents service");
		if (this.handles.has(sessionId)) return this.result(sessionId);
		if (agents.get(sessionId) !== void 0) return {
			managed: false,
			fallback: "restart",
			reason: "the official UI or another component already opened this session"
		};
		if (this.preparing.has(sessionId)) return {
			managed: false,
			fallback: "restart",
			reason: "another prepare-server call is already opening this session"
		};
		this.preparing.add(sessionId);
		try {
			const query = this.ctx.get("sessionQuery");
			if (query === void 0) throw new PluginError("agent-compose-failed", "the profile has no sessionQuery service");
			let observation;
			try {
				observation = await query.observeSession(sessionId);
			} catch (error) {
				return {
					managed: false,
					fallback: "restart",
					reason: `the session cannot be observed before resume: ${error instanceof Error ? error.message : String(error)}`
				};
			}
			try {
				const presets = this.ctx.get("agentPresets");
				const presetId = observation.projections?.values?.agentPreset ?? void 0;
				let resolved;
				try {
					resolved = presets === void 0 ? void 0 : await presets.resolve(presetId);
				} catch (error) {
					throw new PluginError("agent-compose-failed", `cannot resolve agent preset: ${error instanceof Error ? error.message : String(error)}`);
				}
				if (resolved?.broken !== void 0) throw new PluginError("agent-compose-failed", `agent preset is broken: ${resolved.broken}`, { agentPreset: resolved.id });
				const selection = this.ctx.get("agentDefaultModel")?.currentSelection() ?? {};
				const agentOptions = {
					...selection.provider === void 0 ? {} : { provider: selection.provider },
					...selection.model === void 0 ? {} : { model: selection.model },
					...selection.reasoningEffort === void 0 ? {} : { reasoningEffort: selection.reasoningEffort }
				};
				let handle;
				try {
					handle = await agents.resume({
						resumeSessionId: sessionId,
						...Object.keys(agentOptions).length === 0 ? {} : { agentOptions },
						setup: this.setupApproval(resolved === void 0 ? void 0 : async (agentCtx) => {
							await presets?.mount(agentCtx, resolved.id);
						})
					});
				} catch (error) {
					if (error?.name === "SessionAlreadyOwnedError") return {
						managed: false,
						fallback: "restart",
						reason: "another writer already holds this session (unmanaged)"
					};
					throw new PluginError("agent-compose-failed", `resume failed: ${error instanceof Error ? error.message : String(error)}`);
				}
				this.handles.set(sessionId, {
					handle,
					...resolved === void 0 ? {} : { agentPreset: resolved.id }
				});
				return this.result(sessionId);
			} finally {
				observation[Symbol.dispose]?.();
			}
		} finally {
			this.preparing.delete(sessionId);
		}
	}
	/**
	* Create one server-mode blank session whose handle stays with this plugin.
	*
	* This mirrors the official `ApiSessionAgentController.createMinted` path
	* without going through `sessionController.create` (which would publish a
	* live agent whose handle it drops).
	*/
	async createServerBlank(sessionId, cwd, presetId) {
		const agents = this.ctx.get("agents");
		if (agents === void 0) throw new PluginError("agent-compose-failed", "the profile has no agents service");
		if (agents.get(sessionId) !== void 0) throw new PluginError("internal", `session "${sessionId}" already has a live agent`);
		const presets = this.ctx.get("agentPresets");
		let resolved;
		try {
			resolved = presets === void 0 ? void 0 : await presets.resolve(presetId);
		} catch (error) {
			throw new PluginError("agent-compose-failed", `cannot resolve agent preset: ${error instanceof Error ? error.message : String(error)}`);
		}
		if (resolved?.broken !== void 0) throw new PluginError("agent-compose-failed", `agent preset is broken: ${resolved.broken}`, { agentPreset: resolved.id });
		const selection = this.ctx.get("agentDefaultModel")?.currentSelection() ?? {};
		const agentOptions = {
			...selection.provider === void 0 ? {} : { provider: selection.provider },
			...selection.model === void 0 ? {} : { model: selection.model },
			...selection.reasoningEffort === void 0 ? {} : { reasoningEffort: selection.reasoningEffort }
		};
		let handle;
		try {
			handle = await agents.create({
				sessionId,
				meta: {
					cwd,
					...resolved === void 0 ? {} : { agentPreset: resolved.id }
				},
				...Object.keys(agentOptions).length === 0 ? {} : { agentOptions },
				setup: this.setupApproval(resolved === void 0 ? void 0 : async (agentCtx) => {
					await presets?.mount(agentCtx, resolved.id);
				})
			});
		} catch (error) {
			throw new PluginError("agent-compose-failed", `blank session creation failed: ${error instanceof Error ? error.message : String(error)}`);
		}
		this.handles.set(sessionId, {
			handle,
			...resolved === void 0 ? {} : { agentPreset: resolved.id }
		});
		return resolved === void 0 ? {} : { agentPreset: resolved.id };
	}
	/** Whether a live agent exists but the plugin does not own its handle. */
	unmanaged(sessionId) {
		const agents = this.ctx.get("agents");
		return this.handles.has(sessionId) === false && agents?.get(sessionId) !== void 0;
	}
	/** Release the managed handle (cancel + whenIdle + close) and flush the log. */
	async dispose(sessionId) {
		const entry = this.handles.get(sessionId);
		if (entry === void 0) return false;
		this.handles.delete(sessionId);
		await entry.handle.dispose().catch((error) => {
			console.error(`[dsh-remote-workspace-server] managed agent dispose failed for ${sessionId}: ${error instanceof Error ? error.message : String(error)}`);
		});
		await this.ctx.get("sessionPersistence")?.flush().catch(() => {});
		return true;
	}
	/** Switch helper used by executor/switch (server→local). */
	async releaseForSwitch(sessionId, to) {
		if (to === "server") return {
			restartRequired: false,
			disposed: false
		};
		if (this.handles.has(sessionId)) {
			const disposed = await this.dispose(sessionId);
			await this.onSwitch(sessionId, "local");
			return {
				restartRequired: false,
				disposed
			};
		}
		if (this.unmanaged(sessionId)) {
			await this.onSwitch(sessionId, "local");
			return {
				restartRequired: true,
				disposed: false
			};
		}
		await this.onSwitch(sessionId, "local");
		return {
			restartRequired: false,
			disposed: false
		};
	}
	/** Dispose every managed handle (plugin unload / profile shutdown). */
	async disposeAll() {
		for (const sessionId of [...this.handles.keys()]) await this.dispose(sessionId);
	}
	/** Diagnostic list for handshake/profile status. */
	list() {
		return [...this.handles].map(([sessionId, entry]) => ({
			sessionId,
			...entry.agentPreset === void 0 ? {} : { agentPreset: entry.agentPreset }
		}));
	}
	result(sessionId) {
		const entry = this.handles.get(sessionId);
		return {
			managed: true,
			...entry?.agentPreset === void 0 ? {} : { agentPreset: entry.agentPreset }
		};
	}
};
//#endregion
//#region src/server-plugin/executor.ts
/** Per-session executor state shared by every client of one server (protocol §5.4/§16.5). */
/** Durable executor record store (one small JSON file per session). */
var ExecutorStore = class {
	dir;
	constructor(dir) {
		this.dir = dir;
	}
	file(sessionId) {
		return join(this.dir, `${encodeURIComponent(sessionId)}.json`);
	}
	/** Read one record, or null when absent/malformed. */
	async get(sessionId) {
		try {
			const value = JSON.parse(await readFile(this.file(sessionId), "utf8"));
			return value?.version === 1 && typeof value.sessionId === "string" && (value.mode === "server" || value.mode === "local") ? value : null;
		} catch {
			return null;
		}
	}
	/** Write one record atomically. */
	async write(record) {
		await mkdir(this.dir, {
			recursive: true,
			mode: 448
		});
		const path = this.file(record.sessionId);
		const temporary = `${path}.tmp-${process.pid}-${Math.random().toString(16).slice(2, 8)}`;
		await writeFile(temporary, `${JSON.stringify(record)}\n`, { mode: 384 });
		await rename(temporary, path);
		return record;
	}
	/** Upsert a record; owner defaults to the current lease for local mode. */
	async assign(sessionId, mode, owner, switchedBy = "user") {
		return await this.write({
			version: 1,
			sessionId,
			mode,
			updatedAt: (/* @__PURE__ */ new Date()).toISOString(),
			...mode === "local" && owner !== void 0 ? { owner } : {},
			...switchedBy === void 0 ? {} : { switchedBy }
		});
	}
};
/** Matches the official helper's per-frame ceiling. */
const DEFAULT_MAX_FRAME_BYTES = 67108864;
/** Stream data is chunked well below the per-frame ceiling to keep credit accounting fine-grained. */
const DEFAULT_STREAM_CHUNK_BYTES = 262144;
/** Stream ids are 32-bit; ids are allocated per transport, not per peer. */
const MAX_STREAM_ID = 4294967295;
/** Half-close directions on the wire. */
const END_DIRECTION = {
	inbound: 0,
	outbound: 1
};
/** A malformed or oversized frame. */
var FrameError = class extends Error {
	code = "FRAME_PROTOCOL";
	constructor(message) {
		super(message);
		this.name = "FrameError";
	}
};
/** Encode one frame with its 5-byte header. */
function encodeFrame(type, payload) {
	const frame = Buffer.allocUnsafe(5 + payload.length);
	frame.writeUInt8(type, 0);
	frame.writeUInt32BE(payload.length, 1);
	payload.copy(frame, 5);
	return frame;
}
/** Encode an RPC envelope payload; the bytes are already a length-prefixed helper frame. */
function encodeJsonFrame(bytes) {
	return encodeFrame(0, bytes);
}
function withId(id, tail) {
	const payload = Buffer.allocUnsafe(4 + tail.length);
	payload.writeUInt32BE(id >>> 0, 0);
	tail.copy(payload, 4);
	return payload;
}
function jsonTail(value) {
	return Buffer.from(JSON.stringify(value) ?? "null", "utf8");
}
/** `stream-open` with the caller-supplied endpoint coordinates. */
function encodeOpenFrame(id, endpoint) {
	return encodeFrame(1, withId(id, jsonTail({ endpoint })));
}
/** `stream-open` acknowledgement. */
function encodeOpenOkFrame(id) {
	return encodeFrame(5, withId(id, Buffer.alloc(0)));
}
/** `stream-open` refusal; the client rejects its `connectStream()` promise. */
function encodeOpenFailFrame(id, message) {
	return encodeFrame(6, withId(id, jsonTail({ message })));
}
/** One data frame. */
function encodeDataFrame(id, payload) {
	return encodeFrame(2, withId(id, payload));
}
/** Half-close one direction of a stream. */
function encodeEndFrame(id, direction) {
	return encodeFrame(3, withId(id, Buffer.from([direction])));
}
/** Destroy one stream on both ends. */
function encodeAbortFrame(id, message) {
	return encodeFrame(4, withId(id, jsonTail({ message })));
}
/** Replenish the peer's send window. */
function encodeCreditFrame(id, bytes) {
	const tail = Buffer.allocUnsafe(4);
	tail.writeUInt32BE(bytes >>> 0, 0);
	return encodeFrame(7, withId(id, tail));
}
function parseJson(bytes, what) {
	try {
		return JSON.parse(bytes.toString("utf8"));
	} catch {
		throw new FrameError(`malformed ${what} JSON payload`);
	}
}
function objectField(value, field) {
	if (typeof value !== "object" || value === null) return void 0;
	return value[field];
}
/**
* Incremental frame decoder. Feed it transport chunks; it returns every
* complete frame in order. A partial frame at the tail is buffered.
*/
var FrameDecoder = class {
	maxFrameBytes;
	buffer = Buffer.alloc(0);
	constructor(maxFrameBytes = DEFAULT_MAX_FRAME_BYTES) {
		this.maxFrameBytes = maxFrameBytes;
		if (!Number.isSafeInteger(maxFrameBytes) || maxFrameBytes <= 0) throw new FrameError("maxFrameBytes must be a positive integer");
	}
	/** Bytes currently held for an incomplete frame (diagnostics and tests). */
	get pendingBytes() {
		return this.buffer.length;
	}
	/**
	* Decode every complete frame available in `chunk`.
	* @param chunk - transport bytes in arrival order.
	* @returns complete frames, in order.
	* @throws FrameError if a header or payload is malformed.
	*/
	push(chunk) {
		if (chunk.length === 0) return [];
		this.buffer = this.buffer.length === 0 ? chunk : Buffer.concat([this.buffer, chunk]);
		const frames = [];
		let offset = 0;
		while (this.buffer.length - offset >= 5) {
			const type = this.buffer.readUInt8(offset);
			const length = this.buffer.readUInt32BE(offset + 1);
			if (length === 0) throw new FrameError("frame declares an empty payload");
			if (length > this.maxFrameBytes) throw new FrameError(`frame length ${length} exceeds the ${this.maxFrameBytes} byte ceiling`);
			const end = offset + 5 + length;
			if (end > this.buffer.length) break;
			frames.push(this.decode(type, this.buffer.subarray(offset + 5, end)));
			offset = end;
		}
		this.buffer = offset === 0 ? this.buffer : this.buffer.subarray(offset);
		return frames;
	}
	/**
	* Assert the transport ended exactly on a frame boundary.
	* @throws FrameError if a frame is truncated, because the peer's outcome is unknown.
	*/
	end() {
		if (this.buffer.length !== 0) throw new FrameError("transport ended inside a frame");
	}
	decode(type, payload) {
		switch (type) {
			case 0: return {
				type: 0,
				json: payload
			};
			case 1: {
				const { id, tail } = this.readId(payload, "stream-open");
				return {
					type: 1,
					id,
					endpoint: objectField(parseJson(tail, "stream-open"), "endpoint")
				};
			}
			case 5: return {
				type: 5,
				id: this.readId(payload, "stream-open acknowledgement").id
			};
			case 6: {
				const { id, tail } = this.readId(payload, "stream-open refusal");
				return {
					type: 6,
					id,
					message: this.readMessage(tail, "stream-open refusal")
				};
			}
			case 2: {
				const { id, tail } = this.readId(payload, "stream data");
				return {
					type: 2,
					id,
					payload: tail
				};
			}
			case 3: {
				const { id, tail } = this.readId(payload, "stream half-close");
				if (tail.length !== 1) throw new FrameError("stream half-close must carry exactly one direction byte");
				const direction = tail.readUInt8(0);
				if (direction !== END_DIRECTION.inbound && direction !== END_DIRECTION.outbound) throw new FrameError(`unknown stream half-close direction ${direction}`);
				return {
					type: 3,
					id,
					direction
				};
			}
			case 4: {
				const { id, tail } = this.readId(payload, "stream abort");
				return {
					type: 4,
					id,
					message: this.readMessage(tail, "stream abort")
				};
			}
			case 7: {
				const { id, tail } = this.readId(payload, "stream credit");
				if (tail.length !== 4) throw new FrameError("stream credit must carry exactly four bytes");
				return {
					type: 7,
					id,
					bytes: tail.readUInt32BE(0)
				};
			}
			default: throw new FrameError(`unknown frame type ${type}`);
		}
	}
	readId(payload, what) {
		if (payload.length < 4) throw new FrameError(`${what} frame is missing its stream id`);
		return {
			id: payload.readUInt32BE(0),
			tail: payload.subarray(4)
		};
	}
	readMessage(tail, what) {
		const message = objectField(parseJson(tail, what), "message");
		return typeof message === "string" ? message : "";
	}
};
//#endregion
//#region node_modules/.pnpm/zod@4.6.6/node_modules/zod/v4/core/util.js
function getEnumValues(entries) {
	const numericValues = Object.values(entries).filter((v) => typeof v === "number");
	return Object.entries(entries).filter(([k, _]) => numericValues.indexOf(+k) === -1).map(([_, v]) => v);
}
function joinValues(array, separator = "|") {
	return array.map((val) => stringifyPrimitive(val)).join(separator);
}
function jsonStringifyReplacer(_, value) {
	if (typeof value === "bigint") return value.toString();
	return value;
}
var Cached = class {
	constructor(getter) {
		this._getter = getter;
		this._value = void 0;
	}
	get value() {
		const getter = this._getter;
		if (getter !== void 0) {
			this._value = getter();
			this._getter = void 0;
		}
		return this._value;
	}
};
function cached(getter) {
	return new Cached(getter);
}
function nullish(input) {
	return input === null || input === void 0;
}
function cleanRegex(source) {
	const start = source.startsWith("^") ? 1 : 0;
	const end = source.endsWith("$") ? source.length - 1 : source.length;
	return source.slice(start, end);
}
function floatSafeRemainder(val, step) {
	const ratio = val / step;
	const roundedRatio = Math.round(ratio);
	const tolerance = 4 * Number.EPSILON * Math.max(Math.abs(ratio), 1);
	if (Math.abs(ratio - roundedRatio) < tolerance) return 0;
	return ratio - roundedRatio;
}
function assignProp(target, prop, value) {
	Object.defineProperty(target, prop, {
		value,
		writable: true,
		enumerable: true,
		configurable: true
	});
}
/**
* Whichever object a def's `shape` currently answers from: the one the caller passed until the first read, the frozen copy after it.
*
* Its keys and descriptors read without invoking anything, which is what lets a discriminated union check its discriminator, and the cycle walk read a shape, without resolving a getter that references the schema being constructed. A def that answers `shape` from an accessor of its own has none.
*/
function rawShape(def) {
	const desc = Object.getOwnPropertyDescriptor(def, "shape");
	return desc?.get ? desc.get.raw : desc?.value;
}
function sourceShape(schema) {
	return rawShape(schema._zod.def) ?? schema._zod.def.shape;
}
function deferProp(target, key, getter) {
	Object.defineProperty(target, key, {
		get() {
			const value = getter();
			assignProp(this, key, value);
			return value;
		},
		enumerable: true,
		configurable: true
	});
}
function putProp(target, key, value) {
	if (key in target) assignProp(target, key, value);
	else target[key] = value;
}
/**
* Copies `keys` of `source`'s shape onto `target`, each value passed through `wrap`.
*
* A key the source has resolved is copied through now, so the derived shape states it outright and nothing has to resolve it to learn what it holds. A key the source still defers stays deferred, and reads back through the source's own `shape`, so it resolves once and both shapes get that one schema.
*/
function mirrorShape(target, source, keys, wrap) {
	const raw = sourceShape(source);
	for (const key of keys) {
		const desc = Object.getOwnPropertyDescriptor(raw, key);
		if (!desc.enumerable) continue;
		if (desc.get) deferProp(target, key, () => {
			const value = source._zod.def.shape[key];
			return wrap ? wrap(value, key) : value;
		});
		else putProp(target, key, wrap ? wrap(desc.value, key) : desc.value);
	}
}
function mirrorProps(target, source) {
	for (const key of Reflect.ownKeys(source)) {
		const desc = Object.getOwnPropertyDescriptor(source, key);
		if (!desc.enumerable) continue;
		if (desc.get) deferProp(target, key, () => source[key]);
		else putProp(target, key, desc.value);
	}
}
function mergeDefs(...defs) {
	const mergedDescriptors = {};
	for (const def of defs) {
		const descriptors = Object.getOwnPropertyDescriptors(def);
		Object.assign(mergedDescriptors, descriptors);
	}
	return Object.defineProperties({}, mergedDescriptors);
}
function esc(str) {
	return JSON.stringify(str);
}
function slugify(input) {
	return input.toLowerCase().trim().replace(/[^\w\s-]/g, "").replace(/[\s_-]+/g, "-").replace(/^-+|-+$/g, "");
}
const captureStackTrace = "captureStackTrace" in Error ? Error.captureStackTrace : (..._args) => {};
function isObject(data) {
	return typeof data === "object" && data !== null && !Array.isArray(data);
}
const allowsEval = /* @__PURE__*/ cached(() => {
	if (globalConfig.jitless) return false;
	if (typeof navigator !== "undefined" && navigator?.userAgent?.includes("Cloudflare")) return false;
	try {
		new Function("");
		return true;
	} catch (_) {
		return false;
	}
});
function isPlainObject(o) {
	if (isObject(o) === false) return false;
	const ctor = o.constructor;
	if (ctor === void 0) return true;
	if (typeof ctor !== "function") return true;
	const prot = ctor.prototype;
	if (isObject(prot) === false) return false;
	if (Object.prototype.hasOwnProperty.call(prot, "isPrototypeOf") === false) return false;
	return true;
}
function shallowClone(o) {
	if (isPlainObject(o)) return { ...o };
	if (Array.isArray(o)) return [...o];
	if (o instanceof Map) return new Map(o);
	if (o instanceof Set) return new Set(o);
	return o;
}
const propertyKeyTypes = /* @__PURE__*/ new Set([
	"string",
	"number",
	"symbol"
]);
function escapeRegex(str) {
	return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
function clone(inst, def, params) {
	const cl = new inst._zod.constr(def ?? inst._zod.def);
	if (!def || params?.parent) cl._zod.parent = inst;
	return cl;
}
function normalizeParams(_params) {
	const params = _params;
	if (!params) return {};
	if (typeof params === "string") return { error: () => params };
	if (params?.message !== void 0) {
		if (params?.error !== void 0) throw new Error("Cannot specify both `message` and `error` params");
		params.error = params.message;
	}
	delete params.message;
	if (typeof params.error === "string") return {
		...params,
		error: () => params.error
	};
	return params;
}
function stringifyPrimitive(value) {
	if (typeof value === "bigint") return value.toString() + "n";
	if (typeof value === "string") return `"${value}"`;
	return `${value}`;
}
function optionalKeys(shape) {
	return Object.keys(shape).filter((k) => {
		return shape[k]._zod.optin !== void 0 && shape[k]._zod.optout === "optional";
	});
}
const NUMBER_FORMAT_RANGES = /*@__PURE__*/ (() => ({
	safeint: [Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER],
	int32: [-2147483648, 2147483647],
	uint32: [0, 4294967295],
	float32: [-34028234663852886e22, 34028234663852886e22],
	float64: [-Number.MAX_VALUE, Number.MAX_VALUE]
}))();
const BIGINT_FORMAT_RANGES = {
	int64: [/* @__PURE__*/ BigInt("-9223372036854775808"), /* @__PURE__*/ BigInt("9223372036854775807")],
	uint64: [/* @__PURE__*/ BigInt(0), /* @__PURE__*/ BigInt("18446744073709551615")]
};
function pick(schema, mask) {
	const currDef = schema._zod.def;
	const checks = currDef.checks;
	if (checks && checks.length > 0) throw new Error(".pick() cannot be used on object schemas containing refinements");
	const newShape = {};
	mirrorShape(newShape, schema, maskedKeys(schema, mask));
	return clone(schema, mergeDefs(currDef, {
		shape: newShape,
		checks: []
	}));
}
function maskedKeys(schema, mask) {
	const raw = sourceShape(schema);
	const keys = [];
	for (const key of Reflect.ownKeys(mask)) {
		if (!Object.getOwnPropertyDescriptor(raw, key)?.enumerable) throw new Error(`Unrecognized key: "${String(key)}"`);
		if (mask[key]) keys.push(key);
	}
	return keys;
}
function omit(schema, mask) {
	const currDef = schema._zod.def;
	const checks = currDef.checks;
	if (checks && checks.length > 0) throw new Error(".omit() cannot be used on object schemas containing refinements");
	const omitted = new Set(maskedKeys(schema, mask));
	const newShape = {};
	mirrorShape(newShape, schema, Reflect.ownKeys(sourceShape(schema)).filter((key) => !omitted.has(key)));
	return clone(schema, mergeDefs(currDef, {
		shape: newShape,
		checks: []
	}));
}
function extend(schema, shape) {
	if (!isPlainObject(shape)) throw new Error("Invalid input to extend: expected a plain object");
	const checks = schema._zod.def.checks;
	if (checks && checks.length > 0) {
		const existingShape = sourceShape(schema);
		for (const key of Reflect.ownKeys(shape)) if (Object.getOwnPropertyDescriptor(existingShape, key) !== void 0) throw new Error("Cannot overwrite keys on object schemas containing refinements. Use `.safeExtend()` instead.");
	}
	return clone(schema, mergeDefs(schema._zod.def, { shape: extended(schema, shape) }));
}
function extended(schema, shape) {
	const newShape = {};
	mirrorShape(newShape, schema, Reflect.ownKeys(sourceShape(schema)));
	mirrorProps(newShape, shape);
	return newShape;
}
function safeExtend(schema, shape) {
	if (!isPlainObject(shape)) throw new Error("Invalid input to safeExtend: expected a plain object");
	return clone(schema, mergeDefs(schema._zod.def, { shape: extended(schema, shape) }));
}
function merge(a, b) {
	if (!b?._zod?.def) throw new Error("Invalid input to merge: expected an object schema. To merge a plain shape, use `.extend()`.");
	if (a._zod.def.checks?.length) throw new Error(".merge() cannot be used on object schemas containing refinements. Use .safeExtend() instead.");
	const newShape = {};
	mirrorShape(newShape, a, Reflect.ownKeys(sourceShape(a)));
	mirrorShape(newShape, b, Reflect.ownKeys(sourceShape(b)));
	return clone(a, mergeDefs(a._zod.def, {
		shape: newShape,
		get catchall() {
			return b._zod.def.catchall;
		},
		checks: b._zod.def.checks ?? []
	}));
}
function partial(Class, schema, mask, name = "partial") {
	const checks = schema._zod.def.checks;
	if (checks && checks.length > 0) throw new Error(`.${name}() cannot be used on object schemas containing refinements`);
	const selected = mask ? new Set(maskedKeys(schema, mask)) : void 0;
	const newShape = {};
	mirrorShape(newShape, schema, Reflect.ownKeys(sourceShape(schema)), Class && ((value, key) => selected && !selected.has(key) ? value : new Class({
		type: "optional",
		innerType: value
	})));
	return clone(schema, mergeDefs(schema._zod.def, {
		shape: newShape,
		checks: []
	}));
}
function required(Class, schema, mask) {
	const selected = mask ? new Set(maskedKeys(schema, mask)) : void 0;
	const newShape = {};
	mirrorShape(newShape, schema, Reflect.ownKeys(sourceShape(schema)), (value, key) => selected && !selected.has(key) ? value : new Class({
		type: "nonoptional",
		innerType: value
	}));
	return clone(schema, mergeDefs(schema._zod.def, { shape: newShape }));
}
function aborted(x, startIndex = 0) {
	if (x.aborted === true) return true;
	for (let i = startIndex; i < x.issues.length; i++) if (x.issues[i]?.continue !== true) return true;
	return false;
}
function explicitlyAborted(x, startIndex = 0) {
	if (x.aborted === true) return true;
	for (let i = startIndex; i < x.issues.length; i++) if (x.issues[i]?.continue === false) return true;
	return false;
}
function prefixIssues(path, issues) {
	return issues.map((iss) => {
		var _a;
		(_a = iss).path ?? (_a.path = []);
		iss.path.unshift(path);
		return iss;
	});
}
function unwrapMessage(message) {
	return typeof message === "string" ? message : message?.message;
}
/** The leading size checks of `defChecks` that set an upper bound, each paired with it. */
function sizeCaps(defChecks) {
	const caps = [];
	for (const ch of defChecks ?? []) {
		const d = ch._zod.def;
		if (!/_(length|size)$|_equals$/.test(d.check)) break;
		const max = d.maximum ?? d.length ?? d.size;
		if (max >= 0) caps.push([max, ch]);
	}
	return caps;
}
function attachSchema(issues, start, inst) {
	var _a;
	for (let i = start; i < issues.length; i++) (_a = issues[i]).schema ?? (_a.schema = inst);
}
function finalizeIssue(iss, ctx, config) {
	var _a;
	const traits = iss.inst?._zod?.traits;
	if (traits?.has("$ZodType")) {
		if (traits.has("$ZodCheck")) (_a = iss).schema ?? (_a.schema = iss.inst);
		else iss.schema = iss.inst;
	}
	const schemaError = iss.schema !== iss.inst ? iss.schema?._zod.def?.error : void 0;
	const message = iss.message ? iss.message : unwrapMessage(iss.inst?._zod.def?.error?.(iss)) ?? unwrapMessage(schemaError?.(iss)) ?? unwrapMessage(ctx?.error?.(iss)) ?? unwrapMessage(config.customError?.(iss)) ?? unwrapMessage(config.localeError?.(iss)) ?? "Invalid input";
	const full = {};
	for (const k of Object.keys(iss)) {
		if (k === "inst" || k === "schema" || k === "continue" || k === "input" || k === "__proto__") continue;
		full[k] = iss[k];
	}
	full.path ?? (full.path = []);
	full.message = message;
	if (ctx?.reportInput) full.input = iss.input;
	return full;
}
const highSurrogate = /[\uD800-\uDBFF]/;
function codePointLength(str) {
	const units = str.length;
	if (!highSurrogate.test(str)) return units;
	let count = units;
	for (let i = 0; i < units - 1; i++) if ((str.charCodeAt(i) & 64512) === 55296 && (str.charCodeAt(i + 1) & 64512) === 56320) {
		count--;
		i++;
	}
	return count;
}
function getLengthableOrigin(input) {
	if (Array.isArray(input)) return "array";
	if (typeof input === "string") return "string";
	return "unknown";
}
function parsedType(data) {
	const t = typeof data;
	switch (t) {
		case "number": return Number.isNaN(data) ? "nan" : "number";
		case "object": {
			if (data === null) return "null";
			if (Array.isArray(data)) return "array";
			const obj = data;
			if (obj && Object.getPrototypeOf(obj) !== Object.prototype && "constructor" in obj && obj.constructor) return obj.constructor.name;
		}
	}
	return t;
}
function issue(...args) {
	const [iss, input, inst] = args;
	if (typeof iss === "string") return {
		message: iss,
		code: "custom",
		input,
		inst
	};
	return { ...iss };
}
/**
* Installs a trait's members on its prototype. Each value builds that member for the instance on first read; the built value shadows the accessor as an own property, so a detached `const { parse } = schema` keeps working.
*
* Call this from a `proto` initializer, which runs once per prototype — never per instance.
*/
function members(proto, table) {
	for (const key in table) {
		const desc = Object.getOwnPropertyDescriptor(table, key);
		if (desc.get) Object.defineProperty(proto, key, {
			...desc,
			enumerable: false
		});
		else defineBound(proto, key, desc.value);
	}
}
/** Shadows a prototype member with an own value, so a getter that builds from the instance runs once. */
function own(inst, key, value, enumerable = true) {
	Object.defineProperty(inst, key, {
		configurable: true,
		writable: true,
		enumerable,
		value
	});
	return value;
}
/** Like {@link own}, for a member that was never an own data property and has to stay out of `Object.keys`. */
function hide(inst, key, value) {
	return own(inst, key, value, false);
}
/** Adds members a table derives from the instance: each builds on first read and shadows as own data, and assignment shadows the same way, as when these were own properties. */
/* @__NO_SIDE_EFFECTS__ */
function derived(computes, table) {
	for (const key in computes) {
		const compute = computes[key];
		Object.defineProperty(table, key, {
			configurable: true,
			enumerable: true,
			get() {
				return own(this, key, compute(this));
			},
			set(value) {
				own(this, key, value);
			}
		});
	}
	return table;
}
function defineBound(proto, key, fn) {
	Object.defineProperty(proto, key, {
		configurable: true,
		get() {
			return this == null ? fn : own(this, key, fn.bind(this));
		},
		set(value) {
			own(this, key, value);
		}
	});
}
/** Returns the prototype to install on, or `undefined` if this group is already installed on it. */
function claim(inst, sentinel) {
	const proto = Object.getPrototypeOf(inst);
	return sentinel in proto ? void 0 : proto;
}
let installing;
let broke = false;
const breaker = {
	configurable: true,
	get() {
		broke = true;
	}
};
/**
* Installs a lazily-derived internal on the `_zod` prototype of `inst`'s
* constructor, computed from the internals object itself and cached there on
* first read. One accessor per constructor rather than one per instance.
*/
function defineLazyInternal(inst, key, compute) {
	const proto = Object.getPrototypeOf(inst._zod);
	if (key in proto && installing !== inst._zod) {
		installing = void 0;
		return;
	}
	installing = inst._zod;
	Object.defineProperty(proto, key, {
		configurable: true,
		get() {
			Object.defineProperty(this, key, breaker);
			const outer = broke;
			broke = false;
			try {
				const value = compute(this);
				if (broke) delete this[key];
				else Object.defineProperty(this, key, {
					configurable: true,
					writable: true,
					value
				});
				broke = broke || outer;
				return value;
			} catch (err) {
				delete this[key];
				broke = broke || outer;
				throw err;
			}
		},
		set(value) {
			Object.defineProperty(this, key, {
				configurable: true,
				writable: true,
				value
			});
		}
	});
}
/**
* Installs `key` on `inst`'s prototype, computed by `make` on first read and cached there as an own
* data property. One accessor per constructor rather than one per instance, because an own accessor
* puts every instance after the first into v8 dictionary mode. The key doubles as the sentinel.
*/
function installLazyProp(inst, key, make, enumerable) {
	const proto = claim(inst, key);
	if (!proto) return;
	Object.defineProperty(proto, key, {
		configurable: true,
		get() {
			const desc = {
				configurable: true,
				writable: true,
				enumerable,
				value: void 0
			};
			Object.defineProperty(this, key, desc);
			desc.value = make(this);
			Object.defineProperty(this, key, desc);
			return desc.value;
		},
		set(value) {
			Object.defineProperty(this, key, {
				configurable: true,
				writable: true,
				enumerable,
				value
			});
		}
	});
}
/** Marks the thunk `_catch` synthesises for a constant catch value. `Function.length` cannot tell that thunk from a user callback — rest and defaulted parameters both report arity 0 — and a user callback reads `ctx.error`, whose issues only finalize correctly against the caller's per-parse error map. Provenance can say what arity cannot. A plain string key rather than `Symbol.for`, whose call at module scope no bundler can prove pure — the same shape that anchored `urlCanParse` into every build. */
const CONSTANT_CATCH = "~constantCatch";
/** Wraps a constant catch value in a thunk tagged with {@link CONSTANT_CATCH}. */
function constantCatch(value) {
	const fn = () => value;
	fn[CONSTANT_CATCH] = true;
	return fn;
}
//#endregion
//#region node_modules/.pnpm/zod@4.6.6/node_modules/zod/v4/core/core.js
var _a$1;
const _zodDesc = {
	value: void 0,
	enumerable: false
};
let _E = "captureStackTrace" in Error ? Error : null;
function newError(Definition) {
	const E = _E;
	if (E) {
		const saved = E.stackTraceLimit;
		if (typeof saved === "number") {
			try {
				E.stackTraceLimit = 0;
			} catch {
				_E = null;
				return new Definition();
			}
			try {
				return new Definition();
			} finally {
				E.stackTraceLimit = saved;
			}
		}
	}
	return new Definition();
}
/* @__NO_SIDE_EFFECTS__ */
function $constructor(name, initializer, proto, params) {
	const zodProto = {};
	function Internals(def) {
		this.def = def;
		this.constr = _;
		this.traits = /* @__PURE__ */ new Set();
	}
	Internals.prototype = zodProto;
	const protoMembers = proto;
	const initialized = protoMembers && /* @__PURE__ */ new WeakSet();
	function init(inst, def) {
		if (!inst._zod) {
			_zodDesc.value = new Internals(def);
			try {
				Object.defineProperty(inst, "_zod", _zodDesc);
			} finally {
				_zodDesc.value = void 0;
			}
		} else if (inst._zod.traits.has(name)) return;
		inst._zod.traits.add(name);
		initializer(inst, def);
		if (initialized) {
			const own = Object.getPrototypeOf(inst);
			const ctorProto = inst._zod.constr.prototype;
			let up = own;
			while (up && up !== ctorProto) up = Object.getPrototypeOf(up);
			const target = up ?? own;
			if (!initialized.has(target)) {
				initialized.add(target);
				members(target, protoMembers);
			}
		}
		const proto = _.prototype;
		for (const k in proto) {
			if (!Object.prototype.hasOwnProperty.call(proto, k)) continue;
			if (!(k in inst)) inst[k] = proto[k].bind(inst);
		}
	}
	const Parent = params?.Parent ?? Object;
	class Definition extends Parent {}
	Object.defineProperty(Definition, "name", { value: name });
	function _(def) {
		const inst = params?.Parent ? newError(Definition) : this;
		init(inst, def);
		const deferred = inst._zod.deferred;
		if (deferred) {
			for (const fn of deferred) fn();
			inst._zod.deferred = void 0;
		}
		const pp = globalThis.__zod_globalConfig?.postProcessor;
		if (pp) pp(inst);
		return inst;
	}
	Object.defineProperty(_, "init", { value: init });
	Object.defineProperty(_, Symbol.hasInstance, { value: (inst) => {
		if (params?.Parent && inst instanceof params.Parent) return true;
		return inst?._zod?.traits?.has(name);
	} });
	Object.defineProperty(_, "name", { value: name });
	return _;
}
var $ZodAsyncError = class extends Error {
	constructor() {
		super(`Encountered Promise during synchronous parse. Use .parseAsync() instead.`);
	}
};
var $ZodEncodeError = class extends Error {
	constructor(name) {
		super(`Encountered unidirectional transform during encode: ${name}`);
		this.name = "ZodEncodeError";
	}
};
(_a$1 = globalThis).__zod_globalConfig ?? (_a$1.__zod_globalConfig = {});
const globalConfig = globalThis.__zod_globalConfig;
function config(newConfig) {
	if (newConfig) Object.assign(globalConfig, newConfig);
	return globalConfig;
}
//#endregion
//#region node_modules/.pnpm/zod@4.6.6/node_modules/zod/v4/core/errors.js
function _getMessage() {
	const internals = this._zod;
	internals.message ?? (internals.message = JSON.stringify(internals.def, jsonStringifyReplacer, 2));
	return internals.message;
}
function _setMessage(value) {
	this._zod.message = value;
}
const _messageDesc = {
	get: _getMessage,
	set: _setMessage,
	enumerable: true,
	configurable: true
};
const _issuesDesc = {
	value: void 0,
	enumerable: false
};
const _installedToString = /* @__PURE__ */ new WeakSet([Object.prototype, Error.prototype]);
const initializer$1 = (inst, def) => {
	inst.name = "$ZodError";
	_issuesDesc.value = def;
	Object.defineProperty(inst, "issues", _issuesDesc);
	_issuesDesc.value = void 0;
	Object.defineProperty(inst, "message", _messageDesc);
	const proto = Object.getPrototypeOf(inst);
	if (!_installedToString.has(proto)) {
		_installedToString.add(proto);
		Object.defineProperty(proto, "toString", {
			configurable: true,
			enumerable: false,
			get() {
				const value = () => this.message;
				Object.defineProperty(this, "toString", {
					value,
					configurable: true,
					writable: true
				});
				return value;
			},
			set(value) {
				Object.defineProperty(this, "toString", {
					value,
					configurable: true,
					writable: true
				});
			}
		});
	}
};
const $ZodError = /* @__PURE__ */ $constructor("$ZodError", initializer$1);
/** Get-or-create `obj[key]` as an own data property. A path segment naming an inherited member
* ("toString", "constructor") would otherwise read through to the prototype, and assigning
* "__proto__" would hit the setter instead of creating a key. */
function node(obj, key, make) {
	if (!Object.prototype.hasOwnProperty.call(obj, key)) {
		if (key === "__proto__") Object.defineProperty(obj, key, {
			value: make(),
			writable: true,
			enumerable: true,
			configurable: true
		});
		else obj[key] = make();
	}
	return obj[key];
}
function flattenError(error, mapper = (issue) => issue.message) {
	const fieldErrors = {};
	const formErrors = [];
	for (const sub of error.issues) if (sub.path.length > 0) node(fieldErrors, sub.path[0], () => []).push(mapper(sub));
	else formErrors.push(mapper(sub));
	return {
		formErrors,
		fieldErrors
	};
}
function formatError(error, mapper = (issue) => issue.message) {
	const fieldErrors = { _errors: [] };
	const processError = (error, path = []) => {
		for (const issue of error.issues) if (issue.code === "invalid_union" && issue.errors.length) issue.errors.map((issues) => processError({ issues }, [...path, ...issue.path]));
		else if (issue.code === "invalid_key") processError({ issues: issue.issues }, [...path, ...issue.path]);
		else if (issue.code === "invalid_element") processError({ issues: issue.issues }, [...path, ...issue.path]);
		else {
			const fullpath = [...path, ...issue.path];
			if (fullpath.length === 0) fieldErrors._errors.push(mapper(issue));
			else {
				let curr = fieldErrors;
				let i = 0;
				while (i < fullpath.length) {
					const el = fullpath[i];
					const terminal = i === fullpath.length - 1;
					if (el === "_errors") {
						if (terminal) curr._errors.push(mapper(issue));
						i++;
						continue;
					}
					if (!Object.prototype.hasOwnProperty.call(curr, el)) Object.defineProperty(curr, el, {
						value: { _errors: [] },
						enumerable: true,
						writable: true,
						configurable: true
					});
					const node = curr[el];
					if (terminal) node._errors.push(mapper(issue));
					curr = node;
					i++;
				}
			}
		}
	};
	processError(error);
	return fieldErrors;
}
//#endregion
//#region node_modules/.pnpm/zod@4.6.6/node_modules/zod/v4/core/parse.js
function finalizeParams(callee, params) {
	return {
		callee: params?.callee ?? callee,
		Err: params?.Err
	};
}
const _parse = (_Err) => {
	const fn = (schema, value, _ctx, _params) => {
		const ctx = _ctx ? {
			..._ctx,
			async: false
		} : { async: false };
		const result = schema._zod.run({
			value,
			issues: []
		}, ctx);
		if (result instanceof Promise) throw new $ZodAsyncError();
		if (result.issues.length) {
			const e = new ((_params?.Err) ?? _Err)(result.issues.map((iss) => finalizeIssue(iss, ctx, config())));
			captureStackTrace(e, _params?.callee ?? fn);
			throw e;
		}
		return result.value;
	};
	return fn;
};
const _parseAsync = (_Err) => {
	const fn = async (schema, value, _ctx, params) => {
		const ctx = _ctx ? {
			..._ctx,
			async: true
		} : { async: true };
		let result = schema._zod.run({
			value,
			issues: []
		}, ctx);
		if (result instanceof Promise) result = await result;
		if (result.issues.length) {
			const e = new ((params?.Err) ?? _Err)(result.issues.map((iss) => finalizeIssue(iss, ctx, config())));
			captureStackTrace(e, params?.callee ?? fn);
			throw e;
		}
		return result.value;
	};
	return fn;
};
const _safeParse = (_Err) => (schema, value, _ctx) => {
	const ctx = _ctx ? {
		..._ctx,
		async: false
	} : { async: false };
	const result = schema._zod.run({
		value,
		issues: []
	}, ctx);
	if (result instanceof Promise) throw new $ZodAsyncError();
	return result.issues.length ? failure(_Err, result.issues, ctx) : {
		success: true,
		data: result.value
	};
};
function failure(Err, issues, ctx) {
	let error;
	return {
		success: false,
		get error() {
			if (!error) {
				error = new Err(issues.map((iss) => finalizeIssue(iss, ctx, config())));
				issues = void 0;
				ctx = void 0;
			}
			return error;
		},
		set error(e) {
			error = e;
			issues = void 0;
			ctx = void 0;
		}
	};
}
const _safeParseAsync = (_Err) => async (schema, value, _ctx) => {
	const ctx = _ctx ? {
		..._ctx,
		async: true
	} : { async: true };
	let result = schema._zod.run({
		value,
		issues: []
	}, ctx);
	if (result instanceof Promise) result = await result;
	return result.issues.length ? failure(_Err, result.issues, ctx) : {
		success: true,
		data: result.value
	};
};
const COMPILE_INVALID = /* @__PURE__ */ Symbol.for("zod.compile.invalid");
const COMPILE_FALLBACK = /* @__PURE__ */ Symbol.for("zod.compile.fallback");
const validate = ((schema, value, _ctx) => {
	const validator = schema._zod.bag.validator;
	if (validator !== void 0) {
		if (validator(value) !== COMPILE_INVALID) return true;
		if (validator.definite === true && _ctx === void 0) return false;
	}
	return validateFallback(schema, value, _ctx);
});
function validateFallback(schema, value, _ctx) {
	const ctx = _ctx ? {
		..._ctx,
		async: false,
		abortEarly: true
	} : {
		async: false,
		abortEarly: true
	};
	const fallbackRun = schema._zod.bag.fallbackRun;
	let result;
	if (fallbackRun) {
		ctx[COMPILE_FALLBACK] = true;
		result = fallbackRun({
			value,
			issues: []
		}, ctx);
	} else result = schema._zod.run({
		value,
		issues: []
	}, ctx);
	if (result instanceof Promise) throw new $ZodAsyncError();
	return result.issues.length === 0;
}
const validateAsync$1 = async (schema, value, _ctx) => {
	const ctx = _ctx ? {
		..._ctx,
		async: true,
		abortEarly: true
	} : {
		async: true,
		abortEarly: true
	};
	let result = schema._zod.run({
		value,
		issues: []
	}, ctx);
	if (result instanceof Promise) result = await result;
	return result.issues.length === 0;
};
const _encode = (_Err) => {
	const parse = _parse(_Err);
	const fn = (schema, value, _ctx, _params) => {
		const ctx = _ctx ? {
			..._ctx,
			direction: "backward"
		} : { direction: "backward" };
		return parse(schema, value, ctx, finalizeParams(fn, _params));
	};
	return fn;
};
const _decode = (_Err) => {
	const parse = _parse(_Err);
	const fn = (schema, value, _ctx, _params) => {
		return parse(schema, value, _ctx, finalizeParams(fn, _params));
	};
	return fn;
};
const _encodeAsync = (_Err) => {
	const parseAsync = _parseAsync(_Err);
	const fn = async (schema, value, _ctx, _params) => {
		const ctx = _ctx ? {
			..._ctx,
			direction: "backward"
		} : { direction: "backward" };
		return await parseAsync(schema, value, ctx, finalizeParams(fn, _params));
	};
	return fn;
};
const _decodeAsync = (_Err) => {
	const parseAsync = _parseAsync(_Err);
	const fn = async (schema, value, _ctx, _params) => {
		return await parseAsync(schema, value, _ctx, finalizeParams(fn, _params));
	};
	return fn;
};
const _safeEncode = (_Err) => (schema, value, _ctx) => {
	const ctx = _ctx ? {
		..._ctx,
		direction: "backward"
	} : { direction: "backward" };
	return _safeParse(_Err)(schema, value, ctx);
};
const _safeDecode = (_Err) => (schema, value, _ctx) => {
	return _safeParse(_Err)(schema, value, _ctx);
};
const _safeEncodeAsync = (_Err) => async (schema, value, _ctx) => {
	const ctx = _ctx ? {
		..._ctx,
		direction: "backward"
	} : { direction: "backward" };
	return _safeParseAsync(_Err)(schema, value, ctx);
};
const _safeDecodeAsync = (_Err) => async (schema, value, _ctx) => {
	return _safeParseAsync(_Err)(schema, value, _ctx);
};
//#endregion
//#region node_modules/.pnpm/zod@4.6.6/node_modules/zod/v4/core/regexes.js
/**
* @deprecated CUID v1 is deprecated by its authors due to information leakage
* (timestamps embedded in the id). Use {@link cuid2} instead.
* See https://github.com/paralleldrive/cuid.
*/
const cuid = /^[cC][0-9a-z]{6,}$/;
const cuid2 = /^[0-9a-z]+$/;
const ulid = /^[0-7][0-9A-HJKMNP-TV-Za-hjkmnp-tv-z]{25}$/;
const xid = /^[0-9a-vA-V]{20}$/;
const ksuid = /^[A-Za-z0-9]{27}$/;
const nanoid = /^[a-zA-Z0-9_-]{21}$/;
function nanoidOfLength(length) {
	return new RegExp(`^[a-zA-Z0-9_-]{${length}}$`);
}
/** ISO 8601-1 duration regex. Does not support the 8601-2 extensions like negative durations or fractional/negative components. */
const duration = /^P(?:(\d+W)|(?!.*W)(?=\d|T\d)(\d+Y)?(\d+M)?(\d+D)?(T(?=\d)(\d+H)?(\d+M)?(\d+([.,]\d+)?S)?)?)$/;
/** A regex for any UUID-like identifier: 8-4-4-4-12 hex pattern */
const guid = /^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$/;
/** Returns a regex for validating an RFC 9562/4122 UUID.
*
* @param version Optionally specify a version 1-8. If no version is specified, all versions are supported. */
const uuid$1 = (version) => {
	if (!version) return /^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$/;
	return new RegExp(`^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-${version}[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12})$`);
};
/** Practical email validation */
const email = /^(?:[A-Za-z0-9_'+\-]+\.)*[A-Za-z0-9_'+\-]*[A-Za-z0-9_+-]@(?:[A-Za-z0-9][A-Za-z0-9\-]*\.)+[A-Za-z]{2,}$/;
const _emoji$1 = `^(?=[\\s\\S]*[\\p{Extended_Pictographic}\\p{Regional_Indicator}\\u20E3])[\\p{Extended_Pictographic}\\p{Emoji_Component}]+$`;
function emoji() {
	return new RegExp(_emoji$1, "u");
}
const ipv4 = /^(?:(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])$/;
const ipv6 = /^(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:))$/;
const cidrv4 = /^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\/([0-9]|[1-2][0-9]|3[0-2])$/;
const cidrv6 = /^(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:))\/(12[0-8]|1[01][0-9]|[1-9]?[0-9])$/;
const base64$1 = /^$|^(?:[0-9a-zA-Z+/]{4})*(?:(?:[0-9a-zA-Z+/]{2}==)|(?:[0-9a-zA-Z+/]{3}=))?$/;
const base64url = /^(?:[A-Za-z0-9_-]{4})*(?:[A-Za-z0-9_-]{2,3})?$/;
const httpProtocol = /^https?$/;
const e164 = /^\+[1-9]\d{6,14}$/;
const dateSource = `(?:(?:\\d\\d[2468][048]|\\d\\d[13579][26]|\\d\\d0[48]|[02468][048]00|[13579][26]00)-02-29|\\d{4}-(?:(?:0[13578]|1[02])-(?:0[1-9]|[12]\\d|3[01])|(?:0[469]|11)-(?:0[1-9]|[12]\\d|30)|(?:02)-(?:0[1-9]|1\\d|2[0-8])))`;
/** Anchors a pattern source. The interpolation lives here rather than at the call site because
* esbuild will not drop a `@__PURE__` call whose own argument interpolates a variable, but it
* will drop `anchor(dateSource)`. Keeping it inline pinned `date` into every bundle. */
function anchor(source) {
	return new RegExp(`^${source}$`);
}
const date = /*@__PURE__*/ anchor(dateSource);
function timeSource(args) {
	const hhmm = `(?:[01]\\d|2[0-3]):[0-5]\\d`;
	return typeof args.precision === "number" ? args.precision === -1 ? `${hhmm}` : args.precision === 0 ? `${hhmm}:[0-5]\\d` : `${hhmm}:[0-5]\\d\\.\\d{${args.precision}}` : args.seconds ? `${hhmm}:[0-5]\\d(?:\\.\\d+)?` : `${hhmm}(?::[0-5]\\d(?:\\.\\d+)?)?`;
}
function time(args) {
	return new RegExp(`^${timeSource(args)}$`);
}
function datetime(args) {
	const opts = ["Z"];
	if (args.offset) opts.push(`([+-](?:[01]\\d|2[0-3]):[0-5]\\d)`);
	const qualified = `${timeSource({
		precision: args.precision,
		seconds: true
	})}(?:${opts.join("|")})`;
	const timeRegex = args.local ? `${qualified}|${timeSource({ precision: args.precision })}` : qualified;
	return new RegExp(`^${dateSource}T(?:${timeRegex})$`);
}
const anyString = /^[\s\S]{0,}$/;
const integer = /^-?\d+$/;
const number$1 = /^-?\d+(?:\.\d+)?$/;
const boolean$1 = /^(?:true|false)$/i;
const _null$2 = /^null$/i;
const lowercase = /^[^A-Z]*$/;
const uppercase = /^[^a-z]*$/;
//#endregion
//#region node_modules/.pnpm/zod@4.6.6/node_modules/zod/v4/core/checks.js
const $ZodCheck = /*@__PURE__*/ $constructor("$ZodCheck", (inst, def) => {
	var _a;
	inst._zod ?? (inst._zod = {});
	inst._zod.def = def;
	(_a = inst._zod).onattach ?? (_a.onattach = []);
});
/** Default `when` for length-based checks: run only on non-nullish values with a `length`. */
const _whenHasLength = (payload) => {
	const val = payload.value;
	return !nullish(val) && val.length !== void 0;
};
const numericOriginMap = {
	number: "number",
	bigint: "bigint",
	object: "date"
};
const $ZodCheckLessThan = /*@__PURE__*/ $constructor("$ZodCheckLessThan", (inst, def) => {
	$ZodCheck.init(inst, def);
	const origin = numericOriginMap[typeof def.value];
	inst._zod.check = (payload) => {
		if (def.inclusive ? payload.value <= def.value : payload.value < def.value) return;
		payload.issues.push({
			origin: numericOriginMap[typeof payload.value] ?? origin,
			code: "too_big",
			maximum: typeof def.value === "object" ? def.value.getTime() : def.value,
			input: payload.value,
			inclusive: def.inclusive,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodCheckGreaterThan = /*@__PURE__*/ $constructor("$ZodCheckGreaterThan", (inst, def) => {
	$ZodCheck.init(inst, def);
	const origin = numericOriginMap[typeof def.value];
	inst._zod.check = (payload) => {
		if (def.inclusive ? payload.value >= def.value : payload.value > def.value) return;
		payload.issues.push({
			origin: numericOriginMap[typeof payload.value] ?? origin,
			code: "too_small",
			minimum: typeof def.value === "object" ? def.value.getTime() : def.value,
			input: payload.value,
			inclusive: def.inclusive,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodCheckMultipleOf = /*@__PURE__*/ $constructor("$ZodCheckMultipleOf", (inst, def) => {
	$ZodCheck.init(inst, def);
	inst._zod.check = (payload) => {
		if (typeof payload.value !== typeof def.value) throw new Error("Cannot mix number and bigint in multiple_of check.");
		if (typeof payload.value === "bigint" ? def.value !== BigInt(0) && payload.value % def.value === BigInt(0) : floatSafeRemainder(payload.value, def.value) === 0) return;
		payload.issues.push({
			origin: typeof payload.value,
			code: "not_multiple_of",
			divisor: def.value,
			input: payload.value,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodCheckNumberFormat = /*@__PURE__*/ $constructor("$ZodCheckNumberFormat", (inst, def) => {
	$ZodCheck.init(inst, def);
	def.format = def.format || "float64";
	const isInt = def.format?.includes("int");
	const origin = isInt ? "int" : "number";
	const [minimum, maximum] = NUMBER_FORMAT_RANGES[def.format];
	inst._zod.check = (payload) => {
		const input = payload.value;
		if (isInt) {
			if (!Number.isInteger(input)) {
				payload.issues.push({
					expected: origin,
					format: def.format,
					code: "invalid_type",
					continue: false,
					input,
					inst
				});
				return;
			}
			if (!Number.isSafeInteger(input)) {
				if (input > 0) payload.issues.push({
					input,
					code: "too_big",
					maximum: Number.MAX_SAFE_INTEGER,
					note: "Integers must be within the safe integer range.",
					inst,
					origin,
					inclusive: true,
					continue: !def.abort
				});
				else payload.issues.push({
					input,
					code: "too_small",
					minimum: Number.MIN_SAFE_INTEGER,
					note: "Integers must be within the safe integer range.",
					inst,
					origin,
					inclusive: true,
					continue: !def.abort
				});
				return;
			}
		}
		if (input < minimum) payload.issues.push({
			origin: "number",
			input,
			code: "too_small",
			minimum,
			inclusive: true,
			inst,
			continue: !def.abort
		});
		if (input > maximum) payload.issues.push({
			origin: "number",
			input,
			code: "too_big",
			maximum,
			inclusive: true,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodCheckMaxLength = /*@__PURE__*/ $constructor("$ZodCheckMaxLength", (inst, def) => {
	var _a;
	$ZodCheck.init(inst, def);
	(_a = inst._zod.def).when ?? (_a.when = _whenHasLength);
	inst._zod.check = (payload) => {
		const input = payload.value;
		const units = input.length;
		if ((typeof input === "string" && units > def.maximum ? codePointLength(input) : units) <= def.maximum) return;
		const origin = getLengthableOrigin(input);
		payload.issues.push({
			origin,
			code: "too_big",
			maximum: def.maximum,
			inclusive: true,
			input,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodCheckMinLength = /*@__PURE__*/ $constructor("$ZodCheckMinLength", (inst, def) => {
	var _a;
	$ZodCheck.init(inst, def);
	(_a = inst._zod.def).when ?? (_a.when = _whenHasLength);
	inst._zod.check = (payload) => {
		const input = payload.value;
		const units = input.length;
		if ((typeof input === "string" && units >= def.minimum && units < def.minimum * 2 ? codePointLength(input) : units) >= def.minimum) return;
		const origin = getLengthableOrigin(input);
		payload.issues.push({
			origin,
			code: "too_small",
			minimum: def.minimum,
			inclusive: true,
			input,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodCheckLengthEquals = /*@__PURE__*/ $constructor("$ZodCheckLengthEquals", (inst, def) => {
	var _a;
	$ZodCheck.init(inst, def);
	(_a = inst._zod.def).when ?? (_a.when = _whenHasLength);
	inst._zod.check = (payload) => {
		const input = payload.value;
		const units = input.length;
		const length = typeof input === "string" && units >= def.length && units <= def.length * 2 ? codePointLength(input) : units;
		if (length === def.length) return;
		const origin = getLengthableOrigin(input);
		const tooBig = length > def.length;
		payload.issues.push({
			origin,
			...tooBig ? {
				code: "too_big",
				maximum: def.length
			} : {
				code: "too_small",
				minimum: def.length
			},
			inclusive: true,
			exact: true,
			input: payload.value,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodCheckStringFormat = /*@__PURE__*/ $constructor("$ZodCheckStringFormat", (inst, def) => {
	var _a, _b;
	$ZodCheck.init(inst, def);
	if (def.pattern) (_a = inst._zod).check ?? (_a.check = (payload) => {
		def.pattern.lastIndex = 0;
		if (def.pattern.test(payload.value)) return;
		payload.issues.push({
			origin: "string",
			code: "invalid_format",
			format: def.format,
			input: payload.value,
			...def.pattern ? { pattern: def.pattern.toString() } : {},
			inst,
			continue: !def.abort
		});
	});
	else (_b = inst._zod).check ?? (_b.check = () => {});
});
const $ZodCheckRegex = /*@__PURE__*/ $constructor("$ZodCheckRegex", (inst, def) => {
	$ZodCheckStringFormat.init(inst, def);
	inst._zod.check = (payload) => {
		def.pattern.lastIndex = 0;
		if (def.pattern.test(payload.value)) return;
		payload.issues.push({
			origin: "string",
			code: "invalid_format",
			format: "regex",
			input: payload.value,
			pattern: def.pattern.toString(),
			inst,
			continue: !def.abort
		});
	};
});
const $ZodCheckLowerCase = /*@__PURE__*/ $constructor("$ZodCheckLowerCase", (inst, def) => {
	def.pattern ?? (def.pattern = lowercase);
	$ZodCheckStringFormat.init(inst, def);
});
const $ZodCheckUpperCase = /*@__PURE__*/ $constructor("$ZodCheckUpperCase", (inst, def) => {
	def.pattern ?? (def.pattern = uppercase);
	$ZodCheckStringFormat.init(inst, def);
});
const $ZodCheckIncludes = /*@__PURE__*/ $constructor("$ZodCheckIncludes", (inst, def) => {
	$ZodCheck.init(inst, def);
	const escapedRegex = escapeRegex(def.includes);
	def.pattern = new RegExp(typeof def.position === "number" ? `^.{${def.position},}${escapedRegex}` : escapedRegex);
	inst._zod.check = (payload) => {
		if (payload.value.includes(def.includes, def.position)) return;
		payload.issues.push({
			origin: "string",
			code: "invalid_format",
			format: "includes",
			includes: def.includes,
			input: payload.value,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodCheckStartsWith = /*@__PURE__*/ $constructor("$ZodCheckStartsWith", (inst, def) => {
	$ZodCheck.init(inst, def);
	const pattern = new RegExp(`^${escapeRegex(def.prefix)}.*`);
	def.pattern ?? (def.pattern = pattern);
	inst._zod.check = (payload) => {
		if (payload.value.startsWith(def.prefix)) return;
		payload.issues.push({
			origin: "string",
			code: "invalid_format",
			format: "starts_with",
			prefix: def.prefix,
			input: payload.value,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodCheckEndsWith = /*@__PURE__*/ $constructor("$ZodCheckEndsWith", (inst, def) => {
	$ZodCheck.init(inst, def);
	const pattern = new RegExp(`.*${escapeRegex(def.suffix)}$`);
	def.pattern ?? (def.pattern = pattern);
	inst._zod.check = (payload) => {
		if (payload.value.endsWith(def.suffix)) return;
		payload.issues.push({
			origin: "string",
			code: "invalid_format",
			format: "ends_with",
			suffix: def.suffix,
			input: payload.value,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodCheckOverwrite = /*@__PURE__*/ $constructor("$ZodCheckOverwrite", (inst, def) => {
	$ZodCheck.init(inst, def);
	inst._zod.check = (payload) => {
		payload.value = def.tx(payload.value);
	};
});
//#endregion
//#region node_modules/.pnpm/zod@4.6.6/node_modules/zod/v4/core/doc.js
var Doc = class {
	constructor(args = [], closed = {}) {
		this.content = [];
		this.indent = 0;
		this.args = args;
		this.closed = closed;
	}
	indented(fn) {
		this.indent += 1;
		try {
			fn(this);
		} finally {
			this.indent -= 1;
		}
	}
	write(arg) {
		if (typeof arg === "function") {
			arg(this, { execution: "sync" });
			arg(this, { execution: "async" });
			return;
		}
		const lines = arg.split("\n").filter((x) => x);
		const minIndent = Math.min(...lines.map((x) => x.length - x.trimStart().length));
		const dedented = lines.map((x) => x.slice(minIndent)).map((x) => " ".repeat(this.indent * 2) + x);
		for (const line of dedented) this.content.push(line);
	}
	compile() {
		const F = Function;
		const content = this?.content ?? [``];
		return new F(...Object.keys(this.closed), `return function (${this.args.join(", ")}) {\n${content.join("\n")}\n};`)(...Object.values(this.closed));
	}
};
//#endregion
//#region node_modules/.pnpm/zod@4.6.6/node_modules/zod/v4/core/versions.js
const version = {
	major: 4,
	minor: 6,
	patch: 6
};
//#endregion
//#region node_modules/.pnpm/zod@4.6.6/node_modules/zod/v4/core/schemas.js
const $ZodType = /*@__PURE__*/ $constructor("$ZodType", (inst, def) => {
	var _a;
	inst ?? (inst = {});
	inst._zod.def = def;
	inst._zod.bag = inst._zod.bag || {};
	inst._zod.version = version;
	const defChecks = inst._zod.def.checks;
	const checks = inst._zod.traits.has("$ZodCheck") ? [inst, ...defChecks ?? []] : defChecks?.length ? [...defChecks] : [];
	for (const ch of checks) for (const fn of ch._zod.onattach) fn(inst);
	if (checks.length === 0) {
		(_a = inst._zod).deferred ?? (_a.deferred = []);
		inst._zod.deferred?.push(() => {
			inst._zod.run = inst._zod.parse;
		});
	} else {
		const runChecks = (payload, checks, ctx) => {
			if (payload.memo) return payload;
			let isAborted = aborted(payload);
			let asyncResult;
			for (const ch of checks) {
				if (ch._zod.def.when) {
					if (explicitlyAborted(payload)) continue;
					if (!ch._zod.def.when(payload)) continue;
				} else if (isAborted) continue;
				const currLen = payload.issues.length;
				const _ = ch._zod.check(payload);
				if (_ instanceof Promise && ctx?.async === false) throw new $ZodAsyncError();
				if (asyncResult || _ instanceof Promise) asyncResult = (asyncResult ?? Promise.resolve()).then(async () => {
					await _;
					if (payload.issues.length === currLen) return;
					attachSchema(payload.issues, currLen, inst);
					if (!isAborted) isAborted = aborted(payload, currLen);
				});
				else {
					if (payload.issues.length === currLen) continue;
					attachSchema(payload.issues, currLen, inst);
					if (!isAborted) isAborted = aborted(payload, currLen);
				}
			}
			if (asyncResult) return asyncResult.then(() => {
				return payload;
			});
			return payload;
		};
		const handleCanaryResult = (canary, payload, ctx) => {
			if (aborted(canary)) {
				canary.aborted = true;
				return canary;
			}
			const checkResult = runChecks(payload, checks, ctx);
			if (checkResult instanceof Promise) {
				if (ctx.async === false) throw new $ZodAsyncError();
				return checkResult.then((checkResult) => inst._zod.parse(checkResult, ctx));
			}
			return inst._zod.parse(checkResult, ctx);
		};
		inst._zod.run = (payload, ctx) => {
			if (ctx.skipChecks) return inst._zod.parse(payload, ctx);
			if (ctx.direction === "backward") {
				const canary = inst._zod.parse({
					value: payload.value,
					issues: []
				}, {
					...ctx,
					skipChecks: true
				});
				if (canary instanceof Promise) return canary.then((canary) => {
					return handleCanaryResult(canary, payload, ctx);
				});
				return handleCanaryResult(canary, payload, ctx);
			}
			const result = inst._zod.parse(payload, ctx);
			if (result instanceof Promise) {
				if (ctx.async === false) throw new $ZodAsyncError();
				return result.then((result) => runChecks(result, checks, ctx));
			}
			return runChecks(result, checks, ctx);
		};
	}
}, {
	get "~standard"() {
		return hide(this, "~standard", standardProps(this));
	},
	set "~standard"(value) {
		own(this, "~standard", value);
	}
});
/** The Standard Schema surface for `inst`. Shared so wrappers can extend it without forcing it. */
const toStandardResult = (r, ctx) => r.issues.length ? { issues: r.issues.map((iss) => finalizeIssue(iss, ctx, config())) } : { value: r.value };
async function validateAsync(inst, value) {
	const ctx = { async: true };
	return toStandardResult(await inst._zod.run({
		value,
		issues: []
	}, ctx), ctx);
}
function standardProps(inst) {
	return {
		validate: (value) => {
			const ctx = { async: false };
			try {
				const r = inst._zod.run({
					value,
					issues: []
				}, ctx);
				if (!(r instanceof Promise)) return toStandardResult(r, ctx);
			} catch (_) {}
			return validateAsync(inst, value);
		},
		vendor: "zod",
		version: 1
	};
}
const $ZodString = /*@__PURE__*/ $constructor("$ZodString", (inst, def) => {
	$ZodType.init(inst, def);
	inst._zod.pattern = def.pattern ?? anyString;
	inst._zod.parse = (payload, _) => {
		if (def.coerce) try {
			payload.value = String(payload.value);
		} catch (_) {}
		if (typeof payload.value === "string") return payload;
		payload.issues.push({
			expected: "string",
			code: "invalid_type",
			input: payload.value,
			inst
		});
		return payload;
	};
});
const $ZodStringFormat = /*@__PURE__*/ $constructor("$ZodStringFormat", (inst, def) => {
	$ZodCheckStringFormat.init(inst, def);
	$ZodString.init(inst, def);
});
const $ZodGUID = /*@__PURE__*/ $constructor("$ZodGUID", (inst, def) => {
	def.pattern ?? (def.pattern = guid);
	$ZodStringFormat.init(inst, def);
});
const $ZodUUID = /*@__PURE__*/ $constructor("$ZodUUID", (inst, def) => {
	if (def.version) {
		const v = {
			v1: 1,
			v2: 2,
			v3: 3,
			v4: 4,
			v5: 5,
			v6: 6,
			v7: 7,
			v8: 8
		}[def.version];
		if (v === void 0) throw new Error(`Invalid UUID version: "${def.version}"`);
		def.pattern ?? (def.pattern = uuid$1(v));
	} else def.pattern ?? (def.pattern = uuid$1());
	$ZodStringFormat.init(inst, def);
});
const $ZodEmail = /*@__PURE__*/ $constructor("$ZodEmail", (inst, def) => {
	def.pattern ?? (def.pattern = email);
	$ZodStringFormat.init(inst, def);
});
function canParseURL(input) {
	try {
		if (typeof URL !== "undefined" && typeof URL.canParse === "function") return URL.canParse(input);
		new URL(input);
		return true;
	} catch {
		return false;
	}
}
function validateURL(trimmed, def) {
	if (!("normalize" in def) && !("hostname" in def) && !("protocol" in def)) return canParseURL(trimmed) || 2;
	return parseURLObject(trimmed, def);
}
/** Parses a URL while preserving the non-normalizing HTTP guard. */
function parseURLObject(trimmed, def) {
	if (!def.normalize && def.protocol?.source === httpProtocol.source && !/^https?:\/\//i.test(trimmed)) return 1;
	try {
		if (typeof URL !== "undefined") {
			const URLStatic = URL;
			if (typeof URLStatic.parse === "function") return URLStatic.parse(trimmed) ?? 2;
		}
		return new URL(trimmed);
	} catch {
		return 2;
	}
}
const asciiTabOrNewline = /[\t\n\r]/g;
/** The URL parser deletes every ASCII tab, LF and CR from its input before it parses, so `new URL("https://exa\nmple.com")` reports on `example.com`. Applying the same deletion to the returned value closes the half of that divergence which can move the host; the parser's other rewrite, stripping C0 controls at the edges, cannot. */
function stripTabAndNewline(value) {
	return value.replace(asciiTabOrNewline, "");
}
function urlHostnameOk(url, hostname) {
	hostname.lastIndex = 0;
	return hostname.test(url.hostname);
}
function urlProtocolOk(url, protocol) {
	protocol.lastIndex = 0;
	return protocol.test(url.protocol.endsWith(":") ? url.protocol.slice(0, -1) : url.protocol);
}
const $ZodURL = /*@__PURE__*/ $constructor("$ZodURL", (inst, def) => {
	$ZodStringFormat.init(inst, def);
	inst._zod.check = (payload) => {
		try {
			const trimmed = payload.value.trim();
			const url = validateURL(trimmed, def);
			if (url === 1) {
				payload.issues.push({
					code: "invalid_format",
					format: "url",
					note: "Invalid URL format",
					input: payload.value,
					inst,
					continue: !def.abort
				});
				return;
			}
			if (url === 2) {
				payload.issues.push({
					code: "invalid_format",
					format: "url",
					input: payload.value,
					inst,
					continue: !def.abort
				});
				return;
			}
			if (url === true) {
				payload.value = stripTabAndNewline(trimmed);
				return;
			}
			if (def.hostname && !urlHostnameOk(url, def.hostname)) payload.issues.push({
				code: "invalid_format",
				format: "url",
				note: "Invalid hostname",
				pattern: def.hostname.source,
				input: payload.value,
				inst,
				continue: !def.abort
			});
			if (def.protocol && !urlProtocolOk(url, def.protocol)) payload.issues.push({
				code: "invalid_format",
				format: "url",
				note: "Invalid protocol",
				pattern: def.protocol.source,
				input: payload.value,
				inst,
				continue: !def.abort
			});
			payload.value = def.normalize ? url.href : stripTabAndNewline(trimmed);
			return;
		} catch (_) {
			payload.issues.push({
				code: "invalid_format",
				format: "url",
				input: payload.value,
				inst,
				continue: !def.abort
			});
		}
	};
});
const $ZodEmoji = /*@__PURE__*/ $constructor("$ZodEmoji", (inst, def) => {
	def.pattern ?? (def.pattern = emoji());
	$ZodStringFormat.init(inst, def);
});
const $ZodNanoID = /*@__PURE__*/ $constructor("$ZodNanoID", (inst, def) => {
	if (def.length !== void 0 && (!Number.isInteger(def.length) || def.length < 1)) throw new Error(`Invalid nanoid length: ${def.length}`);
	def.pattern ?? (def.pattern = def.length === void 0 ? nanoid : nanoidOfLength(def.length));
	$ZodStringFormat.init(inst, def);
});
/**
* @deprecated CUID v1 is deprecated by its authors due to information leakage
* (timestamps embedded in the id). Use {@link $ZodCUID2} instead.
* See https://github.com/paralleldrive/cuid.
*/
const $ZodCUID = /*@__PURE__*/ $constructor("$ZodCUID", (inst, def) => {
	def.pattern ?? (def.pattern = cuid);
	$ZodStringFormat.init(inst, def);
});
const $ZodCUID2 = /*@__PURE__*/ $constructor("$ZodCUID2", (inst, def) => {
	def.pattern ?? (def.pattern = cuid2);
	$ZodStringFormat.init(inst, def);
});
const $ZodULID = /*@__PURE__*/ $constructor("$ZodULID", (inst, def) => {
	def.pattern ?? (def.pattern = ulid);
	$ZodStringFormat.init(inst, def);
});
const $ZodXID = /*@__PURE__*/ $constructor("$ZodXID", (inst, def) => {
	def.pattern ?? (def.pattern = xid);
	$ZodStringFormat.init(inst, def);
});
const $ZodKSUID = /*@__PURE__*/ $constructor("$ZodKSUID", (inst, def) => {
	def.pattern ?? (def.pattern = ksuid);
	$ZodStringFormat.init(inst, def);
});
const $ZodISODateTime = /*@__PURE__*/ $constructor("$ZodISODateTime", (inst, def) => {
	def.pattern ?? (def.pattern = datetime(def));
	$ZodStringFormat.init(inst, def);
});
const $ZodISODate = /*@__PURE__*/ $constructor("$ZodISODate", (inst, def) => {
	def.pattern ?? (def.pattern = date);
	$ZodStringFormat.init(inst, def);
});
const $ZodISOTime = /*@__PURE__*/ $constructor("$ZodISOTime", (inst, def) => {
	def.pattern ?? (def.pattern = time(def));
	$ZodStringFormat.init(inst, def);
});
const $ZodISODuration = /*@__PURE__*/ $constructor("$ZodISODuration", (inst, def) => {
	def.pattern ?? (def.pattern = duration);
	$ZodStringFormat.init(inst, def);
});
const $ZodIPv4 = /*@__PURE__*/ $constructor("$ZodIPv4", (inst, def) => {
	def.pattern ?? (def.pattern = ipv4);
	$ZodStringFormat.init(inst, def);
});
/** An IPv6 address is written with hex digits, colons and dots, and nothing else. The guard is what makes the check below an IPv6 check: `new URL("http://[...]")` parses an authority, not an address, so `@` and `\` re-delimit it and `"::@1\\"` validates against the host `0.0.0.1`. The URL parser also deletes ASCII tab, LF and CR rather than failing, which is how `"::1\n"` validated as `::1`. */
const ipv6Alphabet = /^[0-9a-fA-F:.]+$/;
function isValidIPv6(value) {
	if (!ipv6Alphabet.test(value)) return false;
	return canParseURL(`http://[${value}]`);
}
const $ZodIPv6 = /*@__PURE__*/ $constructor("$ZodIPv6", (inst, def) => {
	def.pattern ?? (def.pattern = ipv6);
	$ZodStringFormat.init(inst, def);
	inst._zod.check = (payload) => {
		if (!isValidIPv6(payload.value)) payload.issues.push({
			code: "invalid_format",
			format: "ipv6",
			input: payload.value,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodCIDRv4 = /*@__PURE__*/ $constructor("$ZodCIDRv4", (inst, def) => {
	def.pattern ?? (def.pattern = cidrv4);
	$ZodStringFormat.init(inst, def);
});
function isValidCIDRv6(value) {
	const parts = value.split("/");
	if (parts.length !== 2) return false;
	const [address, prefix] = parts;
	if (!prefix) return false;
	const prefixNum = Number(prefix);
	if (`${prefixNum}` !== prefix) return false;
	if (prefixNum < 0 || prefixNum > 128) return false;
	return isValidIPv6(address);
}
const $ZodCIDRv6 = /*@__PURE__*/ $constructor("$ZodCIDRv6", (inst, def) => {
	def.pattern ?? (def.pattern = cidrv6);
	$ZodStringFormat.init(inst, def);
	inst._zod.check = (payload) => {
		if (!isValidCIDRv6(payload.value)) payload.issues.push({
			code: "invalid_format",
			format: "cidrv6",
			input: payload.value,
			inst,
			continue: !def.abort
		});
	};
});
function isValidBase64(data) {
	if (data === "") return true;
	if (/\s/.test(data)) return false;
	if (data.length % 4 !== 0) return false;
	try {
		atob(data);
		return true;
	} catch {
		return false;
	}
}
const base64Charset = /^[0-9a-zA-Z+/]*={0,2}$/;
const $ZodBase64 = /*@__PURE__*/ $constructor("$ZodBase64", (inst, def) => {
	def.pattern ?? (def.pattern = base64Charset);
	$ZodStringFormat.init(inst, def);
	inst._zod.check = (payload) => {
		if (isValidBase64(payload.value)) return;
		payload.issues.push({
			code: "invalid_format",
			format: "base64",
			input: payload.value,
			inst,
			continue: !def.abort
		});
	};
});
const base64urlCharset = /^[A-Za-z0-9_-]*$/;
function isValidBase64URL(data) {
	if (!base64urlCharset.test(data)) return false;
	const base64 = data.replace(/[-_]/g, (c) => c === "-" ? "+" : "/");
	return isValidBase64(base64.padEnd(Math.ceil(base64.length / 4) * 4, "="));
}
const $ZodBase64URL = /*@__PURE__*/ $constructor("$ZodBase64URL", (inst, def) => {
	def.pattern ?? (def.pattern = base64urlCharset);
	$ZodStringFormat.init(inst, def);
	inst._zod.check = (payload) => {
		if (isValidBase64URL(payload.value)) return;
		payload.issues.push({
			code: "invalid_format",
			format: "base64url",
			input: payload.value,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodE164 = /*@__PURE__*/ $constructor("$ZodE164", (inst, def) => {
	def.pattern ?? (def.pattern = e164);
	$ZodStringFormat.init(inst, def);
});
function isValidJWT(token, algorithm = null) {
	try {
		const tokensParts = token.split(".");
		if (tokensParts.length !== 3) return false;
		const [header] = tokensParts;
		if (!header) return false;
		const parsedHeader = JSON.parse(atob(header));
		if ("typ" in parsedHeader && parsedHeader?.typ !== "JWT") return false;
		if (!parsedHeader.alg) return false;
		if (algorithm && (!("alg" in parsedHeader) || parsedHeader.alg !== algorithm)) return false;
		return true;
	} catch {
		return false;
	}
}
const $ZodJWT = /*@__PURE__*/ $constructor("$ZodJWT", (inst, def) => {
	$ZodStringFormat.init(inst, def);
	inst._zod.check = (payload) => {
		if (isValidJWT(payload.value, def.alg)) return;
		payload.issues.push({
			code: "invalid_format",
			format: "jwt",
			input: payload.value,
			inst,
			continue: !def.abort
		});
	};
});
const $ZodNumber = /*@__PURE__*/ $constructor("$ZodNumber", (inst, def) => {
	$ZodType.init(inst, def);
	inst._zod.pattern = number$1;
	inst._zod.parse = (payload, _ctx) => {
		if (def.coerce) try {
			payload.value = Number(payload.value);
		} catch (_) {}
		const input = payload.value;
		if (typeof input === "number" && !Number.isNaN(input) && Number.isFinite(input)) return payload;
		const received = typeof input === "number" ? Number.isNaN(input) ? "NaN" : !Number.isFinite(input) ? String(input) : void 0 : void 0;
		payload.issues.push({
			expected: "number",
			code: "invalid_type",
			input,
			inst,
			...received ? { received } : {}
		});
		return payload;
	};
});
const $ZodNumberFormat = /*@__PURE__*/ $constructor("$ZodNumberFormat", (inst, def) => {
	$ZodCheckNumberFormat.init(inst, def);
	$ZodNumber.init(inst, def);
});
const $ZodBoolean = /*@__PURE__*/ $constructor("$ZodBoolean", (inst, def) => {
	$ZodType.init(inst, def);
	inst._zod.pattern = boolean$1;
	inst._zod.parse = (payload, _ctx) => {
		if (def.coerce) try {
			payload.value = Boolean(payload.value);
		} catch (_) {}
		const input = payload.value;
		if (typeof input === "boolean") return payload;
		payload.issues.push({
			expected: "boolean",
			code: "invalid_type",
			input,
			inst
		});
		return payload;
	};
});
const $ZodNull = /*@__PURE__*/ $constructor("$ZodNull", (inst, def) => {
	$ZodType.init(inst, def);
	inst._zod.pattern = _null$2;
	inst._zod.values = /* @__PURE__ */ new Set([null]);
	inst._zod.parse = (payload, _ctx) => {
		const input = payload.value;
		if (input === null) return payload;
		payload.issues.push({
			expected: "null",
			code: "invalid_type",
			input,
			inst
		});
		return payload;
	};
});
const $ZodUnknown = /*@__PURE__*/ $constructor("$ZodUnknown", (inst, def) => {
	$ZodType.init(inst, def);
	inst._zod.parse = (payload) => payload;
});
const $ZodNever = /*@__PURE__*/ $constructor("$ZodNever", (inst, def) => {
	$ZodType.init(inst, def);
	inst._zod.parse = (payload, _ctx) => {
		payload.issues.push({
			expected: "never",
			code: "invalid_type",
			input: payload.value,
			inst
		});
		return payload;
	};
});
function capSize(inst, size) {
	const caps = sizeCaps(inst._zod.def.checks);
	if (!caps.length) return;
	const cap = Math.min(...caps.map((c) => c[0]));
	const parse = inst._zod.parse;
	inst._zod.parse = (payload, ctx) => {
		const n = size(payload.value);
		if (n > cap && !ctx?.skipChecks && ctx?.direction !== "backward") {
			for (const [max, ch] of sizeCaps(inst._zod.def.checks)) if (n > max && ch._zod.def.when(payload)) {
				const start = payload.issues.length;
				ch._zod.check(payload);
				payload.issues[start].continue = false;
				attachSchema(payload.issues, start, inst);
				return payload;
			}
		}
		return parse(payload, ctx);
	};
}
const arraySize = (input) => Array.isArray(input) ? input.length : -1;
function handleArrayResult(result, final, index) {
	if (result.issues.length) final.issues.push(...prefixIssues(index, result.issues));
	final.value[index] = result.value;
}
const $ZodArray = /*@__PURE__*/ $constructor("$ZodArray", (inst, def) => {
	$ZodType.init(inst, def);
	const memo = globalConfig.memoizer;
	memo?.attach(inst);
	inst._zod.parse = (payload, ctx) => {
		const input = payload.value;
		if (!Array.isArray(input)) {
			payload.issues.push({
				expected: "array",
				code: "invalid_type",
				input,
				inst
			});
			return payload;
		}
		payload.value = memo ? memo.alloc(inst, payload, Array(input.length), ctx) : Array(input.length);
		const proms = [];
		const abortEarly = ctx?.abortEarly;
		for (let i = 0; i < input.length; i++) {
			const item = input[i];
			const result = def.element._zod.run({
				value: item,
				issues: []
			}, ctx);
			if (result instanceof Promise) proms.push(result.then((result) => handleArrayResult(result, payload, i)));
			else {
				handleArrayResult(result, payload, i);
				if (abortEarly && result.issues.length !== 0 && aborted(result)) break;
			}
		}
		if (proms.length) return Promise.all(proms).then(() => payload);
		return payload;
	};
	capSize(inst, arraySize);
});
function handlePropertyResult(result, final, key, input, optin, optout) {
	const isPresent = key in input;
	const isOptionalOut = optout === "optional";
	if (!isPresent && isOptionalOut && optin === "optional") return;
	if (result.issues.length) {
		if (optin !== void 0 && isOptionalOut && !isPresent) return;
		final.issues.push(...prefixIssues(key, result.issues));
	}
	if (!isPresent && optin === void 0) {
		if (!result.issues.length) final.issues.push({
			code: "invalid_type",
			expected: "nonoptional",
			input: void 0,
			path: [key]
		});
		return;
	}
	if (result.value === void 0) {
		if (isPresent || optin === "defaulted" && !isOptionalOut) final.value[key] = void 0;
	} else final.value[key] = result.value;
}
const NO_SYMBOL_KEYS = [];
function normalizeDef(def) {
	const keys = Object.keys(def.shape);
	const ownSymbols = Object.getOwnPropertySymbols(def.shape);
	const symbolKeys = ownSymbols.length ? ownSymbols : NO_SYMBOL_KEYS;
	const allKeys = symbolKeys.length ? [...keys, ...symbolKeys] : keys;
	for (const k of allKeys) if (!def.shape?.[k]?._zod?.traits?.has("$ZodType")) throw new Error(`Invalid element at key "${String(k)}": expected a Zod schema`);
	const okeys = optionalKeys(def.shape);
	return {
		...def,
		allKeys,
		symbolKeys,
		keySet: new Set(keys),
		numKeys: keys.length,
		optionalKeys: new Set(okeys)
	};
}
function handleCatchall(proms, input, payload, ctx, def, inst, abortEarly) {
	const unrecognized = [];
	const keySet = def.keySet;
	const _catchall = def.catchall._zod;
	const t = _catchall.def.type;
	const optin = _catchall.optin;
	const optout = _catchall.optout;
	let seen = 0;
	for (const key in input) {
		if (abortEarly && payload.issues.length !== seen) {
			if (aborted(payload, seen)) break;
			seen = payload.issues.length;
		}
		if (keySet.has(key)) continue;
		if (key === "__proto__") {
			if (t === "never") unrecognized.push(key);
			continue;
		}
		if (t === "never") {
			unrecognized.push(key);
			continue;
		}
		const r = _catchall.run({
			value: input[key],
			issues: []
		}, ctx);
		if (r instanceof Promise) proms.push(r.then((r) => handlePropertyResult(r, payload, key, input, optin, optout)));
		else handlePropertyResult(r, payload, key, input, optin, optout);
	}
	if (unrecognized.length) payload.issues.push({
		code: "unrecognized_keys",
		keys: unrecognized,
		input,
		inst,
		continue: true
	});
	if (!proms.length) return payload;
	return Promise.all(proms).then(() => {
		return payload;
	});
}
const $ZodObject = /*@__PURE__*/ $constructor("$ZodObject", (inst, def) => {
	$ZodType.init(inst, def);
	const desc = Object.getOwnPropertyDescriptor(def, "shape");
	const sh = desc?.get ? desc.get.raw : def.shape ?? {};
	if (sh) {
		const get = () => {
			const newSh = { ...sh };
			Object.defineProperty(def, "shape", { value: newSh });
			get.raw = newSh;
			return newSh;
		};
		get.raw = sh;
		Object.defineProperty(def, "shape", { get });
	}
	const _normalized = cached(() => normalizeDef(def));
	defineLazyInternal(inst, "propValues", (zod) => {
		const shape = zod.def.shape;
		const propValues = {};
		for (const key in shape) {
			const field = shape[key]._zod;
			if (field.values) {
				if (!Object.prototype.hasOwnProperty.call(propValues, key)) assignProp(propValues, key, /* @__PURE__ */ new Set());
				for (const v of field.values) propValues[key].add(v);
				if (field.optin !== void 0) propValues[key].add(void 0);
			}
		}
		return propValues;
	});
	const isObject$1 = isObject;
	const catchall = def.catchall;
	let value;
	const memo = globalConfig.memoizer;
	memo?.attach(inst);
	inst._zod.parse = (payload, ctx) => {
		value ?? (value = _normalized.value);
		const input = payload.value;
		if (!isObject$1(input)) {
			payload.issues.push({
				expected: "object",
				code: "invalid_type",
				input,
				inst
			});
			return payload;
		}
		payload.value = memo ? memo.alloc(inst, payload, {}, ctx) : {};
		const proms = [];
		const shape = value.shape;
		const abortEarly = ctx?.abortEarly;
		let seen = payload.issues.length;
		for (const key of value.allKeys) {
			if (abortEarly && payload.issues.length !== seen) {
				if (aborted(payload, seen)) break;
				seen = payload.issues.length;
			}
			if (key === "__proto__") continue;
			const el = shape[key];
			const optin = el._zod.optin;
			const optout = el._zod.optout;
			const r = el._zod.run({
				value: input[key],
				issues: []
			}, ctx);
			if (r instanceof Promise) proms.push(r.then((r) => handlePropertyResult(r, payload, key, input, optin, optout)));
			else handlePropertyResult(r, payload, key, input, optin, optout);
		}
		if (!catchall) return proms.length ? Promise.all(proms).then(() => payload) : payload;
		return handleCatchall(proms, input, payload, ctx, _normalized.value, inst, abortEarly === true);
	};
});
const $ZodObjectJIT = /*@__PURE__*/ $constructor("$ZodObjectJIT", (inst, def) => {
	$ZodObject.init(inst, def);
	const superParse = inst._zod.parse;
	const _normalized = cached(() => normalizeDef(def));
	const memo = globalConfig.memoizer;
	const generateFastpass = (shape) => {
		const normalized = _normalized.value;
		const syms = normalized.symbolKeys;
		const doc = new Doc(["payload", "ctx"], {
			shape,
			inst,
			memo,
			syms
		});
		const parseStr = (k) => `shape[${k}]._zod.run({ value: input[${k}], issues: [] }, ctx)`;
		const prefixStr = (id, k) => `
          let ${id}_ab = false;
          for (let i = 0; i < ${id}.issues.length; i++) {
            const iss = ${id}.issues[i];
            iss.path = iss.path ? [${k}, ...iss.path] : [${k}];
            payload.issues.push(iss);
            if (iss.continue !== true) ${id}_ab = true;
          }
          if (${id}_ab && ctx && ctx.abortEarly) {
            payload.value = newResult;
            return payload;
          }`;
		doc.write(`const input = payload.value;`);
		const ids = Object.create(null);
		let counter = 0;
		for (const key of normalized.allKeys) ids[key] = `key_${counter++}`;
		doc.write(memo ? `const newResult = memo.alloc(inst, payload, {}, ctx);` : `const newResult = {};`);
		for (const key of normalized.allKeys) {
			if (key === "__proto__") continue;
			const id = ids[key];
			const k = typeof key === "symbol" ? `syms[${syms.indexOf(key)}]` : esc(key);
			const isPresent = `${k} in input`;
			const schema = shape[key];
			const optin = schema?._zod?.optin;
			const isOptionalIn = optin !== void 0;
			const isOptionalOut = schema?._zod?.optout === "optional";
			doc.write(`const ${id} = ${parseStr(k)};`);
			if (isOptionalIn && isOptionalOut) {
				const assign = optin === "optional" ? `${id}_present` : `${id}.value !== undefined || ${id}_present`;
				doc.write(`
        const ${id}_present = ${isPresent};
        if (!${id}.issues.length || ${id}_present) {
          if (${id}.issues.length) {${prefixStr(id, k)}
          }

          if (${assign}) {
            newResult[${k}] = ${id}.value;
          }
        }

      `);
			} else if (!isOptionalIn) doc.write(`
        const ${id}_present = ${isPresent};
        if (${id}.issues.length) {${prefixStr(id, k)}
        }
        if (!${id}_present && !${id}.issues.length) {
          payload.issues.push({
            code: "invalid_type",
            expected: "nonoptional",
            input: undefined,
            path: [${k}]
          });
          if (ctx && ctx.abortEarly) {
            payload.value = newResult;
            return payload;
          }
        }

        if (${id}_present) {
          newResult[${k}] = ${id}.value;
        }

      `);
			else {
				doc.write(`
        if (${id}.issues.length) {${prefixStr(id, k)}
        }
      `);
				if (optin === "defaulted") doc.write(`newResult[${k}] = ${id}.value;`);
				else doc.write(`
        if (${id}.value !== undefined || ${isPresent}) {
          newResult[${k}] = ${id}.value;
        }
      `);
			}
		}
		doc.write(`payload.value = newResult;`);
		doc.write(`return payload;`);
		return doc.compile();
	};
	let fastpass;
	const isObject$2 = isObject;
	const jit = !globalConfig.jitless;
	const fastEnabled = jit && allowsEval.value;
	const catchall = def.catchall;
	let value;
	inst._zod.parse = (payload, ctx) => {
		value ?? (value = _normalized.value);
		const input = payload.value;
		if (!isObject$2(input)) {
			payload.issues.push({
				expected: "object",
				code: "invalid_type",
				input,
				inst
			});
			return payload;
		}
		if (jit && fastEnabled && ctx?.async === false && ctx.jitless !== true) {
			if (!fastpass) fastpass = generateFastpass(def.shape);
			payload = fastpass(payload, ctx);
			if (!catchall) return payload;
			return handleCatchall([], input, payload, ctx, value, inst, ctx?.abortEarly === true);
		}
		return superParse(payload, ctx);
	};
});
function handleUnionResults(results, final, inst, ctx) {
	for (const result of results) if (result.issues.length === 0) {
		final.value = result.value;
		return final;
	}
	const nonaborted = results.filter((r) => !aborted(r));
	if (nonaborted.length === 1) {
		final.value = nonaborted[0].value;
		return nonaborted[0];
	}
	final.issues.push({
		code: "invalid_union",
		input: final.value,
		inst,
		errors: results.map((result) => result.issues.map((iss) => finalizeIssue(iss, ctx, config())))
	});
	return final;
}
const $ZodUnion = /*@__PURE__*/ $constructor("$ZodUnion", (inst, def) => {
	$ZodType.init(inst, def);
	defineLazyInternal(inst, "optin", (zod) => zod.def.options.some((o) => o._zod.optin === "defaulted") ? "defaulted" : zod.def.options.some((o) => o._zod.optin !== void 0) ? "optional" : void 0);
	defineLazyInternal(inst, "optout", (zod) => zod.def.options.some((o) => o._zod.optout === "optional") ? "optional" : void 0);
	defineLazyInternal(inst, "values", (zod) => {
		if (zod.def.options.every((o) => o._zod.values)) return new Set(zod.def.options.flatMap((option) => Array.from(option._zod.values)));
	});
	defineLazyInternal(inst, "pattern", (zod) => {
		if (zod.def.options.every((o) => o._zod.pattern)) {
			const patterns = zod.def.options.map((o) => o._zod.pattern);
			return new RegExp(`^(${patterns.map((p) => cleanRegex(p.source)).join("|")})$`);
		}
	});
	const first = def.options.length === 1 ? def.options[0]._zod.run : null;
	inst._zod.parse = (payload, ctx) => {
		if (first) return first(payload, ctx);
		let async = false;
		const results = [];
		for (const option of def.options) {
			const result = option._zod.run({
				value: payload.value,
				issues: []
			}, ctx);
			if (result instanceof Promise) {
				results.push(result);
				async = true;
			} else {
				if (result.issues.length === 0) return result;
				results.push(result);
			}
		}
		if (!async) return handleUnionResults(results, payload, inst, ctx);
		return Promise.all(results).then((results) => {
			return handleUnionResults(results, payload, inst, ctx);
		});
	};
});
function discriminatorMap(def) {
	const map = /* @__PURE__ */ new Map();
	for (const option of def.options) {
		const values = option._zod.propValues?.[def.discriminator];
		if (!values || values.size === 0) throw new Error(`Invalid discriminated union option at index "${def.options.indexOf(option)}"`);
		for (const value of values) if (map.has(value)) {
			if (value !== void 0) throw new Error(`Duplicate discriminator value "${String(value)}"`);
			map.set(value, null);
		} else map.set(value, option);
	}
	return map;
}
const $ZodDiscriminatedUnion = /*@__PURE__*/ $constructor("$ZodDiscriminatedUnion", (inst, def) => {
	def.inclusive = false;
	$ZodUnion.init(inst, def);
	const _super = inst._zod.parse;
	defineLazyInternal(inst, "propValues", (zod) => {
		const propValues = {};
		let undefinedCount = 0;
		for (const option of zod.def.options) {
			const pv = option._zod.propValues;
			if (!pv || Object.keys(pv).length === 0) throw new Error(`Invalid discriminated union option at index "${zod.def.options.indexOf(option)}"`);
			if (pv[zod.def.discriminator]?.has(void 0)) undefinedCount++;
			for (const [k, v] of Object.entries(pv)) {
				if (!Object.prototype.hasOwnProperty.call(propValues, k)) assignProp(propValues, k, /* @__PURE__ */ new Set());
				for (const val of v) propValues[k].add(val);
			}
		}
		if (!zod.def.unionFallback && undefinedCount > 1) propValues[zod.def.discriminator]?.delete(void 0);
		return propValues;
	});
	def.options.forEach((option, i) => {
		const propShape = rawShape(option._zod.def);
		if (propShape && !Object.prototype.hasOwnProperty.call(propShape, def.discriminator)) throw new Error(`Invalid discriminated union option at index "${i}"`);
	});
	const disc = cached(() => discriminatorMap(def));
	inst._zod.parse = (payload, ctx) => {
		const input = payload.value;
		if (!isObject(input)) {
			payload.issues.push({
				code: "invalid_type",
				expected: "object",
				input,
				inst
			});
			return payload;
		}
		const value = input?.[def.discriminator];
		const opt = disc.value.get(value);
		if (opt && (value !== void 0 || ctx.direction !== "backward")) return opt._zod.run(payload, ctx);
		if (def.unionFallback || ctx.direction === "backward") return _super(payload, ctx);
		payload.issues.push({
			code: "invalid_union",
			errors: [],
			note: "No matching discriminator",
			discriminator: def.discriminator,
			options: Array.from(disc.value.keys()).filter((value) => disc.value.get(value) !== null),
			input,
			path: [def.discriminator],
			inst
		});
		return payload;
	};
});
const $ZodIntersection = /*@__PURE__*/ $constructor("$ZodIntersection", (inst, def) => {
	$ZodType.init(inst, def);
	inst._zod.parse = (payload, ctx) => {
		const input = payload.value;
		const left = def.left._zod.run({
			value: input,
			issues: []
		}, ctx);
		const right = def.right._zod.run({
			value: input,
			issues: []
		}, ctx);
		if (left instanceof Promise || right instanceof Promise) return Promise.all([left, right]).then(([left, right]) => {
			return handleIntersectionResults(payload, left, right);
		});
		return handleIntersectionResults(payload, left, right);
	};
});
function mergeValues(a, b) {
	if (a === b) return {
		valid: true,
		data: a
	};
	if (a instanceof Date && b instanceof Date && +a === +b) return {
		valid: true,
		data: a
	};
	if (isPlainObject(a) && isPlainObject(b)) {
		const bKeys = Object.keys(b);
		const sharedKeys = Object.keys(a).filter((key) => bKeys.indexOf(key) !== -1);
		const newObj = {
			...a,
			...b
		};
		if (Object.prototype.hasOwnProperty.call(newObj, "__proto__")) delete newObj.__proto__;
		for (const key of sharedKeys) {
			if (key === "__proto__") continue;
			const sharedValue = mergeValues(a[key], b[key]);
			if (!sharedValue.valid) return {
				valid: false,
				mergeErrorPath: [key, ...sharedValue.mergeErrorPath]
			};
			newObj[key] = sharedValue.data;
		}
		return {
			valid: true,
			data: newObj
		};
	}
	if (Array.isArray(a) && Array.isArray(b)) {
		if (a.length !== b.length) return {
			valid: false,
			mergeErrorPath: []
		};
		const newArray = [];
		for (let index = 0; index < a.length; index++) {
			const itemA = a[index];
			const itemB = b[index];
			const sharedValue = mergeValues(itemA, itemB);
			if (!sharedValue.valid) return {
				valid: false,
				mergeErrorPath: [index, ...sharedValue.mergeErrorPath]
			};
			newArray.push(sharedValue.data);
		}
		return {
			valid: true,
			data: newArray
		};
	}
	return {
		valid: false,
		mergeErrorPath: []
	};
}
function handleIntersectionResults(result, left, right) {
	const unrecKeys = /* @__PURE__ */ new Map();
	let unrecIssue;
	const keyIssues = /* @__PURE__ */ new Map();
	const collect = (iss, side) => {
		let keys;
		if (iss.code === "unrecognized_keys" && !iss.path?.length) {
			unrecIssue ?? (unrecIssue = iss);
			keys = iss.keys;
		} else if (iss.code === "invalid_key" && iss.origin === "record" && iss.path?.length === 1) {
			const k = String(iss.path[0]);
			if (!keyIssues.has(k)) keyIssues.set(k, iss);
			keys = [k];
		} else return false;
		for (const k of keys) {
			if (!unrecKeys.has(k)) unrecKeys.set(k, {});
			unrecKeys.get(k)[side] = true;
		}
		return true;
	};
	for (const iss of left.issues) if (!collect(iss, "l")) result.issues.push(iss);
	for (const iss of right.issues) if (!collect(iss, "r")) result.issues.push(iss);
	const bothKeys = [...unrecKeys].filter(([, f]) => f.l && f.r).map(([k]) => k);
	if (bothKeys.length) {
		const aggregated = unrecIssue ? bothKeys.filter((k) => unrecIssue.keys.includes(k)) : [];
		if (aggregated.length) result.issues.push({
			...unrecIssue,
			keys: aggregated
		});
		for (const k of bothKeys) if (!aggregated.includes(k) && keyIssues.has(k)) result.issues.push(keyIssues.get(k));
	}
	const merged = mergeValues(left.value, right.value);
	if (!merged.valid) {
		if (aborted(result)) return result;
		throw new Error(`Unmergable intersection. Error path: ${JSON.stringify(merged.mergeErrorPath)}`);
	}
	result.value = merged.data;
	return result;
}
const $ZodRecord = /*@__PURE__*/ $constructor("$ZodRecord", (inst, def) => {
	$ZodType.init(inst, def);
	const memo = globalConfig.memoizer;
	memo?.attach(inst);
	inst._zod.parse = (payload, ctx) => {
		const input = payload.value;
		if (!isPlainObject(input)) {
			payload.issues.push({
				expected: "record",
				code: "invalid_type",
				input,
				inst
			});
			return payload;
		}
		const proms = [];
		const values = def.keyType._zod.values;
		if (values && !def.partial) {
			payload.value = memo ? memo.alloc(inst, payload, {}, ctx) : {};
			const recordKeys = /* @__PURE__ */ new Set();
			for (const key of values) if (typeof key === "string" || typeof key === "number" || typeof key === "symbol") {
				recordKeys.add(typeof key === "number" ? key.toString() : key);
				if (key === "__proto__") continue;
				const keyResult = def.keyType._zod.run({
					value: key,
					issues: []
				}, ctx);
				if (keyResult instanceof Promise) throw new Error("Async schemas not supported in object keys currently");
				if (keyResult.issues.length) {
					payload.issues.push({
						code: "invalid_key",
						origin: "record",
						issues: keyResult.issues.map((iss) => finalizeIssue(iss, ctx, config())),
						input: key,
						path: [key],
						inst
					});
					continue;
				}
				const outKey = keyResult.value;
				if (outKey === "__proto__") continue;
				const result = def.valueType._zod.run({
					value: input[key],
					issues: []
				}, ctx);
				if (result instanceof Promise) proms.push(result.then((result) => {
					if (result.issues.length) payload.issues.push(...prefixIssues(key, result.issues));
					payload.value[outKey] = result.value;
				}));
				else {
					if (result.issues.length) payload.issues.push(...prefixIssues(key, result.issues));
					payload.value[outKey] = result.value;
				}
			}
			let unrecognized;
			for (const key in input) if (!recordKeys.has(key)) {
				if (def.mode === "loose") {
					if (key === "__proto__") continue;
					payload.value[key] = input[key];
				} else {
					unrecognized = unrecognized ?? [];
					unrecognized.push(key);
				}
			}
			if (unrecognized && unrecognized.length > 0) payload.issues.push({
				code: "unrecognized_keys",
				input,
				inst,
				keys: unrecognized,
				continue: true
			});
		} else {
			payload.value = memo ? memo.alloc(inst, payload, {}, ctx) : {};
			let unrecognized;
			for (const key of Reflect.ownKeys(input)) {
				if (key === "__proto__") continue;
				if (!Object.prototype.propertyIsEnumerable.call(input, key)) continue;
				let keyResult = def.keyType._zod.run({
					value: key,
					issues: []
				}, ctx);
				if (keyResult instanceof Promise) throw new Error("Async schemas not supported in object keys currently");
				if (typeof key === "string" && number$1.test(key) && keyResult.issues.length) {
					const retryResult = def.keyType._zod.run({
						value: Number(key),
						issues: []
					}, ctx);
					if (retryResult instanceof Promise) throw new Error("Async schemas not supported in object keys currently");
					if (retryResult.issues.length === 0) keyResult = retryResult;
				}
				if (keyResult.issues.length) {
					if (def.mode === "loose") payload.value[key] = input[key];
					else if (values) {
						unrecognized = unrecognized ?? [];
						unrecognized.push(key);
					} else payload.issues.push({
						code: "invalid_key",
						origin: "record",
						issues: keyResult.issues.map((iss) => finalizeIssue(iss, ctx, config())),
						input: key,
						path: [key],
						inst
					});
					continue;
				}
				const outKey = keyResult.value;
				if (outKey === "__proto__") continue;
				const result = def.valueType._zod.run({
					value: input[key],
					issues: []
				}, ctx);
				if (result instanceof Promise) proms.push(result.then((result) => {
					if (result.issues.length) payload.issues.push(...prefixIssues(key, result.issues));
					payload.value[outKey] = result.value;
				}));
				else {
					if (result.issues.length) payload.issues.push(...prefixIssues(key, result.issues));
					payload.value[outKey] = result.value;
				}
			}
			if (unrecognized && unrecognized.length > 0) payload.issues.push({
				code: "unrecognized_keys",
				input,
				inst,
				keys: unrecognized,
				continue: true
			});
		}
		if (proms.length) return Promise.all(proms).then(() => payload);
		return payload;
	};
});
const $ZodEnum = /*@__PURE__*/ $constructor("$ZodEnum", (inst, def) => {
	$ZodType.init(inst, def);
	const values = getEnumValues(def.entries);
	const valuesSet = new Set(values);
	inst._zod.values = valuesSet;
	defineLazyInternal(inst, "pattern", (zod) => {
		const patternValues = getEnumValues(zod.def.entries).filter((k) => propertyKeyTypes.has(typeof k));
		return new RegExp(patternValues.length ? `^(${patternValues.map((o) => escapeRegex(o.toString())).join("|")})$` : "^[^\\s\\S]$");
	});
	inst._zod.parse = (payload, _ctx) => {
		const input = payload.value;
		if (valuesSet.has(input)) return payload;
		payload.issues.push({
			code: "invalid_value",
			values,
			input,
			inst
		});
		return payload;
	};
});
const $ZodLiteral = /*@__PURE__*/ $constructor("$ZodLiteral", (inst, def) => {
	$ZodType.init(inst, def);
	const values = new Set(def.values);
	inst._zod.values = values;
	defineLazyInternal(inst, "pattern", (zod) => {
		const vals = zod.def.values;
		return new RegExp(vals.length ? `^(${vals.map((o) => typeof o === "string" ? escapeRegex(o) : o ? escapeRegex(o.toString()) : String(o)).join("|")})$` : "^[^\\s\\S]$");
	});
	inst._zod.parse = (payload, _ctx) => {
		const input = payload.value;
		if (values.has(input)) return payload;
		payload.issues.push({
			code: "invalid_value",
			values: def.values,
			input,
			inst
		});
		return payload;
	};
});
const $ZodTransform = /*@__PURE__*/ $constructor("$ZodTransform", (inst, def) => {
	$ZodType.init(inst, def);
	inst._zod.optin = "optional";
	globalConfig.memoizer?.guard(inst);
	inst._zod.parse = (payload, ctx) => {
		if (ctx.direction === "backward") throw new $ZodEncodeError(inst.constructor.name);
		const _out = def.transform(payload.value, payload);
		if (ctx.async) return (_out instanceof Promise ? _out : Promise.resolve(_out)).then((output) => {
			payload.value = output;
			return payload;
		});
		if (_out instanceof Promise) throw new $ZodAsyncError();
		payload.value = _out;
		return payload;
	};
});
function handleOptionalResult(payload, result) {
	payload.value = result.issues.length ? void 0 : result.value;
	return payload;
}
const $ZodOptional = /*@__PURE__*/ $constructor("$ZodOptional", (inst, def) => {
	$ZodType.init(inst, def);
	defineLazyInternal(inst, "optin", (zod) => zod.def.innerType._zod.optin === "defaulted" ? "defaulted" : "optional");
	inst._zod.optout = "optional";
	defineLazyInternal(inst, "values", (zod) => {
		const values = zod.def.innerType._zod.values;
		return values ? /* @__PURE__ */ new Set([...values, void 0]) : void 0;
	});
	defineLazyInternal(inst, "pattern", (zod) => {
		const pattern = zod.def.innerType._zod.pattern;
		return pattern ? new RegExp(`^(${cleanRegex(pattern.source)})?$`) : void 0;
	});
	inst._zod.parse = (payload, ctx) => {
		if (payload.value === void 0) {
			if (def.innerType._zod.optin !== "defaulted") return payload;
			const result = def.innerType._zod.run({
				value: payload.value,
				issues: []
			}, ctx);
			if (result instanceof Promise) return result.then((result) => handleOptionalResult(payload, result));
			return handleOptionalResult(payload, result);
		}
		return def.innerType._zod.run(payload, ctx);
	};
});
const $ZodExactOptional = /*@__PURE__*/ $constructor("$ZodExactOptional", (inst, def) => {
	$ZodOptional.init(inst, def);
	defineLazyInternal(inst, "values", (zod) => zod.def.innerType._zod.values);
	defineLazyInternal(inst, "pattern", (zod) => zod.def.innerType._zod.pattern);
	inst._zod.parse = (payload, ctx) => {
		return def.innerType._zod.run(payload, ctx);
	};
});
const $ZodNullable = /*@__PURE__*/ $constructor("$ZodNullable", (inst, def) => {
	$ZodType.init(inst, def);
	defineLazyInternal(inst, "optin", (zod) => zod.def.innerType._zod.optin);
	defineLazyInternal(inst, "optout", (zod) => zod.def.innerType._zod.optout);
	defineLazyInternal(inst, "pattern", (zod) => {
		const pattern = zod.def.innerType._zod.pattern;
		return pattern ? new RegExp(`^(${cleanRegex(pattern.source)}|null)$`) : void 0;
	});
	defineLazyInternal(inst, "values", (zod) => {
		return zod.def.innerType._zod.values ? /* @__PURE__ */ new Set([...zod.def.innerType._zod.values, null]) : void 0;
	});
	inst._zod.parse = (payload, ctx) => {
		if (payload.value === null) return payload;
		return def.innerType._zod.run(payload, ctx);
	};
});
const $ZodDefault = /*@__PURE__*/ $constructor("$ZodDefault", (inst, def) => {
	$ZodType.init(inst, def);
	inst._zod.optin = "defaulted";
	defineLazyInternal(inst, "values", (zod) => zod.def.innerType._zod.values);
	inst._zod.parse = (payload, ctx) => {
		if (ctx.direction === "backward") return def.innerType._zod.run(payload, ctx);
		if (payload.value === void 0) {
			payload.value = def.defaultValue;
			/**
			* $ZodDefault returns the default value immediately in forward direction.
			* It doesn't pass the default value into the validator ("prefault"). There's no reason to pass the default value through validation. The validity of the default is enforced by TypeScript statically. Otherwise, it's the responsibility of the user to ensure the default is valid. In the case of pipes with divergent in/out types, you can specify the default on the `in` schema of your ZodPipe to set a "prefault" for the pipe.   */
			return payload;
		}
		const result = def.innerType._zod.run(payload, ctx);
		if (result instanceof Promise) return result.then((result) => handleDefaultResult(result, def));
		return handleDefaultResult(result, def);
	};
});
function handleDefaultResult(payload, def) {
	if (payload.value === void 0) payload.value = def.defaultValue;
	return payload;
}
const $ZodPrefault = /*@__PURE__*/ $constructor("$ZodPrefault", (inst, def) => {
	$ZodType.init(inst, def);
	inst._zod.optin = "defaulted";
	defineLazyInternal(inst, "values", (zod) => zod.def.innerType._zod.values);
	inst._zod.parse = (payload, ctx) => {
		if (ctx.direction === "backward") return def.innerType._zod.run(payload, ctx);
		if (payload.value === void 0) payload.value = def.defaultValue;
		return def.innerType._zod.run(payload, ctx);
	};
});
const $ZodNonOptional = /*@__PURE__*/ $constructor("$ZodNonOptional", (inst, def) => {
	$ZodType.init(inst, def);
	defineLazyInternal(inst, "values", (zod) => {
		const v = zod.def.innerType._zod.values;
		return v ? new Set([...v].filter((x) => x !== void 0)) : void 0;
	});
	inst._zod.parse = (payload, ctx) => {
		const result = def.innerType._zod.run(payload, ctx);
		if (result instanceof Promise) return result.then((result) => handleNonOptionalResult(result, inst));
		return handleNonOptionalResult(result, inst);
	};
});
function handleNonOptionalResult(payload, inst) {
	if (!payload.issues.length && payload.value === void 0) payload.issues.push({
		code: "invalid_type",
		expected: "nonoptional",
		input: payload.value,
		inst
	});
	return payload;
}
function handleCatchResult(payload, result, def, ctx) {
	if (!result.issues.length) {
		payload.value = result.value;
		if (result.memo) payload.memo = true;
		return payload;
	}
	payload.value = def.catchValue({
		...result,
		value: payload.value,
		error: { issues: result.issues.map((iss) => finalizeIssue(iss, ctx, config())) },
		input: payload.value
	});
	return payload;
}
const $ZodCatch = /*@__PURE__*/ $constructor("$ZodCatch", (inst, def) => {
	$ZodType.init(inst, def);
	defineLazyInternal(inst, "optin", (zod) => zod.def.innerType._zod.optin === "defaulted" ? "defaulted" : "optional");
	defineLazyInternal(inst, "optout", (zod) => zod.def.innerType._zod.optout);
	defineLazyInternal(inst, "values", (zod) => zod.def.innerType._zod.values);
	inst._zod.parse = (payload, ctx) => {
		if (ctx.direction === "backward") return def.innerType._zod.run(payload, ctx);
		const result = def.innerType._zod.run({
			value: payload.value,
			issues: []
		}, ctx);
		if (result instanceof Promise) return result.then((result) => handleCatchResult(payload, result, def, ctx));
		return handleCatchResult(payload, result, def, ctx);
	};
});
const $ZodPipe = /*@__PURE__*/ $constructor("$ZodPipe", (inst, def) => {
	$ZodType.init(inst, def);
	defineLazyInternal(inst, "values", (zod) => zod.def.in._zod.values);
	defineLazyInternal(inst, "optin", (zod) => zod.def.in._zod.optin);
	defineLazyInternal(inst, "optout", (zod) => zod.def.out._zod.optout);
	defineLazyInternal(inst, "propValues", (zod) => zod.def.in._zod.propValues);
	inst._zod.parse = (payload, ctx) => {
		if (ctx.direction === "backward") {
			const right = def.out._zod.run(payload, ctx);
			if (right instanceof Promise) return right.then((right) => handlePipeResult(right, def.in, ctx));
			return handlePipeResult(right, def.in, ctx);
		}
		const left = def.in._zod.run(payload, ctx);
		if (left instanceof Promise) return left.then((left) => handlePipeResult(left, def.out, ctx));
		return handlePipeResult(left, def.out, ctx);
	};
});
function handlePipeResult(left, next, ctx) {
	if (left.issues.some((iss) => iss.code !== "unrecognized_keys")) {
		left.aborted = true;
		return left;
	}
	return next._zod.run({
		value: left.value,
		issues: left.issues
	}, ctx);
}
const $ZodReadonly = /*@__PURE__*/ $constructor("$ZodReadonly", (inst, def) => {
	$ZodType.init(inst, def);
	defineLazyInternal(inst, "propValues", (zod) => zod.def.innerType._zod.propValues);
	defineLazyInternal(inst, "values", (zod) => zod.def.innerType._zod.values);
	defineLazyInternal(inst, "optin", (zod) => zod.def.innerType?._zod?.optin);
	defineLazyInternal(inst, "optout", (zod) => zod.def.innerType?._zod?.optout);
	inst._zod.parse = (payload, ctx) => {
		if (ctx.direction === "backward") return def.innerType._zod.run(payload, ctx);
		const result = def.innerType._zod.run(payload, ctx);
		if (result instanceof Promise) return result.then(handleReadonlyResult);
		return handleReadonlyResult(result);
	};
});
function handleReadonlyResult(payload) {
	if (!payload.memo) payload.value = Object.freeze(payload.value);
	return payload;
}
const $ZodCustom = /*@__PURE__*/ $constructor("$ZodCustom", (inst, def) => {
	$ZodCheck.init(inst, def);
	$ZodType.init(inst, def);
	inst._zod.parse = (payload, _) => {
		return payload;
	};
	inst._zod.check = (payload) => {
		const input = payload.value;
		const r = def.fn(input);
		if (r instanceof Promise) return r.then((r) => handleRefineResult(r, payload, input, inst));
		handleRefineResult(r, payload, input, inst);
	};
});
function handleRefineResult(result, payload, input, inst) {
	if (!result) {
		const _iss = {
			code: "custom",
			input,
			inst,
			path: [...inst._zod.def.path ?? []],
			continue: !inst._zod.def.abort
		};
		if (inst._zod.def.params) _iss.params = inst._zod.def.params;
		payload.issues.push(issue(_iss));
	}
}
//#endregion
//#region node_modules/.pnpm/zod@4.6.6/node_modules/zod/v4/core/memoizer.js
var $ZodCyclicError = class extends Error {
	constructor() {
		super(`Cannot parse a reference cycle that closes through a transform`);
		this.name = "ZodCyclicError";
	}
};
/** Keyed off the context object every schema in one parse call already shares. */
const STATE = "~memo";
const NO_ISSUES = [];
function isRef(value) {
	return value !== null && typeof value === "object";
}
function cloneIssues(issues) {
	return issues.map((iss) => iss.path ? {
		...iss,
		path: iss.path.slice()
	} : { ...iss });
}
const recursive = /*@__PURE__*/ new WeakMap();
/** What the walk established, in order of certainty: ordered so the strongest answer among children wins. */
const NONE = 0;
const ASSUMED = 1;
const PROVEN = 2;
/** Whether this schema's subtree contains a cycle, so one parse can re-enter it. */
function isRecursive(inst, stack, resolve) {
	const cached = recursive.get(inst);
	if (cached !== void 0) return cached ? PROVEN : NONE;
	if (stack.has(inst)) return PROVEN;
	stack.add(inst);
	let result = NONE;
	const check = (child) => {
		if (result !== PROVEN && child?._zod) {
			const answer = isRecursive(child, stack, resolve);
			if (answer > result) result = answer;
		}
	};
	const shape = (sh, spread) => {
		let answer = NONE;
		for (const key of Reflect.ownKeys(sh)) {
			const desc = Object.getOwnPropertyDescriptor(sh, key);
			if (spread && !desc.enumerable) continue;
			const child = desc.get ? ASSUMED : desc.value?._zod ? isRecursive(desc.value, stack, resolve) : NONE;
			if (child > answer) answer = child;
		}
		return answer;
	};
	const merge = (answer) => {
		if (answer > result) result = answer;
	};
	const def = inst._zod.def;
	switch (def.type) {
		case "object": {
			const raw = rawShape(def);
			merge(raw ? shape(raw, true) : ASSUMED);
			check(def.catchall);
			break;
		}
		case "array":
			check(def.element);
			break;
		case "tuple":
			for (const el of def.items) check(el);
			check(def.rest);
			break;
		case "record":
		case "map":
			check(def.keyType);
			check(def.valueType);
			break;
		case "set":
			check(def.valueType);
			break;
		case "union":
			for (const el of def.options) check(el);
			break;
		case "intersection":
			check(def.left);
			check(def.right);
			break;
		case "optional":
		case "nullable":
		case "default":
		case "prefault":
		case "catch":
		case "readonly":
		case "nonoptional":
		case "promise":
		case "success":
			check(def.innerType);
			break;
		case "pipe":
			check(def.in);
			check(def.out);
			break;
		case "function":
			check(def.input);
			check(def.output);
			break;
		case "lazy": {
			const inner = def._cachedInner ?? (resolve ? inst._zod.innerType : void 0);
			merge(inner ? isRecursive(inner, stack, false) : ASSUMED);
			break;
		}
		case "template_literal":
		case "string":
		case "number":
		case "int":
		case "boolean":
		case "bigint":
		case "symbol":
		case "undefined":
		case "null":
		case "void":
		case "never":
		case "any":
		case "unknown":
		case "date":
		case "nan":
		case "enum":
		case "literal":
		case "file":
		case "transform":
		case "custom": break;
		default: for (const key in def) {
			const desc = Object.getOwnPropertyDescriptor(def, key);
			if (!desc || desc.get) continue;
			const value = desc.value;
			if (!value || typeof value !== "object") continue;
			if (value._zod) check(value);
			else if (Array.isArray(value)) for (const el of value) check(el);
		}
	}
	stack.delete(inst);
	return settle(inst, result);
}
/** An assumed answer must not outlive the resolution that settles it, so only a certain one is cached. */
function settle(inst, answer) {
	if (answer !== ASSUMED) recursive.set(inst, answer === PROVEN);
	return answer;
}
function bucketFor(state, inst) {
	let bucket = state.buckets.get(inst);
	if (!bucket) {
		bucket = /* @__PURE__ */ new WeakMap();
		state.buckets.set(inst, bucket);
	}
	return bucket;
}
let handoff;
const open$1 = [];
const memo = {
	alloc(_inst, payload, empty) {
		const bucket = handoff;
		if (!bucket) return empty;
		handoff = void 0;
		const entry = {
			value: empty,
			issues: null
		};
		bucket.set(payload.value, entry);
		open$1.push(entry);
		return empty;
	},
	guard(inst) {
		var _a;
		(_a = inst._zod).deferred ?? (_a.deferred = []);
		inst._zod.deferred.push(() => {
			const base = inst._zod.parse;
			const wrapped = (payload, ctx) => {
				if (ctx.direction !== "backward" && isBackEdge(ctx, payload.value)) throw new $ZodCyclicError();
				return base(payload, ctx);
			};
			inst._zod.parse = wrapped;
			if (inst._zod.run === base) inst._zod.run = wrapped;
		});
	},
	attach(inst) {
		var _a;
		let isRecursiveInst;
		let rechecked = false;
		let lastCtx;
		let lastBucket;
		(_a = inst._zod).deferred ?? (_a.deferred = []);
		inst._zod.deferred.push(() => {
			const base = inst._zod.parse;
			const wrapped = (payload, ctx) => {
				if (isRecursiveInst === void 0) {
					const walked = isRecursive(inst, /* @__PURE__ */ new Set(), false);
					if (walked === NONE) {
						inst._zod.parse = base;
						if (inst._zod.run === wrapped) inst._zod.run = base;
						return base(payload, ctx);
					}
					if (walked === PROVEN || rechecked) isRecursiveInst = true;
					else rechecked = true;
				}
				const input = payload.value;
				if (!isRef(input)) return base(payload, ctx);
				let state = ctx[STATE];
				if (!state) {
					state = {
						buckets: /* @__PURE__ */ new WeakMap(),
						backEdges: void 0
					};
					ctx[STATE] = state;
				}
				let bucket;
				if (lastCtx === ctx) bucket = lastBucket;
				else {
					bucket = bucketFor(state, inst);
					lastCtx = ctx;
					lastBucket = bucket;
				}
				const hit = bucket.get(input);
				if (hit) {
					payload.value = hit.value;
					if (hit.issues) {
						if (hit.issues.length) payload.issues.push(...cloneIssues(hit.issues));
					} else {
						payload.memo = true;
						state.backEdges ?? (state.backEdges = /* @__PURE__ */ new WeakSet());
						state.backEdges.add(hit.value);
					}
					return payload;
				}
				handoff = bucket;
				const depth = open$1.length;
				const result = base(payload, ctx);
				handoff = void 0;
				const entry = open$1.length > depth ? open$1.pop() : void 0;
				if (result instanceof Promise) return result.then((r) => {
					if (entry) entry.issues = r.issues.length ? cloneIssues(r.issues) : NO_ISSUES;
					return r;
				});
				if (entry) entry.issues = result.issues.length ? cloneIssues(result.issues) : NO_ISSUES;
				return result;
			};
			inst._zod.parse = wrapped;
			if (inst._zod.run === base) inst._zod.run = wrapped;
		});
	}
};
/** The memoizer that gives containers cycle support. `zod` installs it by default; `zod/mini` opts in with `config({ memoizer: memoizer() })`. */
function memoizer() {
	return memo;
}
/** Whether this value is a node a back-edge resolved to before it finished. */
function isBackEdge(ctx, value) {
	const backEdges = ctx[STATE]?.backEdges;
	return backEdges !== void 0 && isRef(value) && backEdges.has(value);
}
//#endregion
//#region node_modules/.pnpm/zod@4.6.6/node_modules/zod/v4/locales/en.js
const error = () => {
	const Sizable = {
		string: {
			unit: "characters",
			verb: "to have"
		},
		file: {
			unit: "bytes",
			verb: "to have"
		},
		array: {
			unit: "items",
			verb: "to have"
		},
		set: {
			unit: "items",
			verb: "to have"
		},
		map: {
			unit: "entries",
			verb: "to have"
		}
	};
	function getSizing(origin) {
		return Sizable[origin] ?? null;
	}
	const FormatDictionary = {
		regex: "input",
		email: "email address",
		url: "URL",
		emoji: "emoji",
		uuid: "UUID",
		uuidv4: "UUIDv4",
		uuidv6: "UUIDv6",
		nanoid: "nanoid",
		guid: "GUID",
		cuid: "cuid",
		cuid2: "cuid2",
		ulid: "ULID",
		xid: "XID",
		ksuid: "KSUID",
		datetime: "ISO datetime",
		date: "ISO date",
		time: "ISO time",
		duration: "ISO duration",
		ipv4: "IPv4 address",
		ipv6: "IPv6 address",
		mac: "MAC address",
		cidrv4: "IPv4 range",
		cidrv6: "IPv6 range",
		base64: "base64-encoded string",
		base64url: "base64url-encoded string",
		json_string: "JSON string",
		e164: "E.164 number",
		currency_code: "currency code",
		credit_card: "credit card number",
		iban: "IBAN",
		jwt: "JWT",
		template_literal: "input"
	};
	const TypeDictionary = { nan: "NaN" };
	function getTypeName(type, input) {
		if (type === "number" && typeof input === "number" && !Number.isFinite(input)) return String(input);
		return TypeDictionary[type] ?? type;
	}
	return (issue) => {
		switch (issue.code) {
			case "invalid_type": return `Invalid input: expected ${getTypeName(issue.expected)}, received ${getTypeName(parsedType(issue.input), issue.input)}`;
			case "invalid_value":
				if (issue.values.length === 1) return `Invalid input: expected ${stringifyPrimitive(issue.values[0])}`;
				return `Invalid option: expected one of ${joinValues(issue.values, "|")}`;
			case "too_big": {
				const adj = issue.exact ? "exactly " : issue.inclusive ? "<=" : "<";
				const sizing = getSizing(issue.origin);
				if (sizing) return `Too big: expected ${issue.origin ?? "value"} to have ${adj}${issue.maximum.toString()} ${sizing.unit ?? "elements"}`;
				return `Too big: expected ${issue.origin ?? "value"} to be ${adj}${issue.maximum.toString()}`;
			}
			case "too_small": {
				const adj = issue.exact ? "exactly " : issue.inclusive ? ">=" : ">";
				const sizing = getSizing(issue.origin);
				if (sizing) return `Too small: expected ${issue.origin} to have ${adj}${issue.minimum.toString()} ${sizing.unit}`;
				return `Too small: expected ${issue.origin} to be ${adj}${issue.minimum.toString()}`;
			}
			case "invalid_format": {
				const _issue = issue;
				if (_issue.format === "starts_with") return `Invalid string: must start with "${_issue.prefix}"`;
				if (_issue.format === "ends_with") return `Invalid string: must end with "${_issue.suffix}"`;
				if (_issue.format === "includes") return `Invalid string: must include "${_issue.includes}"`;
				if (_issue.format === "regex") return `Invalid string: must match pattern ${_issue.pattern}`;
				return `Invalid ${FormatDictionary[_issue.format] ?? issue.format}`;
			}
			case "not_multiple_of": return `Invalid number: must be a multiple of ${issue.divisor}`;
			case "unrecognized_keys": return `Unrecognized key${issue.keys.length > 1 ? "s" : ""}: ${joinValues(issue.keys, ", ")}`;
			case "invalid_key": return `Invalid key in ${issue.origin}`;
			case "invalid_union":
				if (issue.options && Array.isArray(issue.options) && issue.options.length > 0) return `Invalid discriminator value. Expected ${issue.options.map((o) => `'${o}'`).join(" | ")}`;
				if (issue.inclusive === false) return "Invalid input: more than one option matched";
				return "Invalid input";
			case "invalid_element": return `Invalid value in ${issue.origin}`;
			default: return `Invalid input`;
		}
	};
};
function en_default() {
	return { localeError: error() };
}
//#endregion
//#region node_modules/.pnpm/zod@4.6.6/node_modules/zod/v4/core/registries.js
var _a;
var $ZodRegistry = class {
	constructor() {
		this._map = /* @__PURE__ */ new WeakMap();
		this._idmap = /* @__PURE__ */ new Map();
	}
	add(schema, ..._meta) {
		const meta = _meta[0];
		this._map.set(schema, meta);
		if (meta && typeof meta === "object" && "id" in meta) this._idmap.set(meta.id, schema);
		return this;
	}
	clear() {
		this._map = /* @__PURE__ */ new WeakMap();
		this._idmap = /* @__PURE__ */ new Map();
		return this;
	}
	remove(schema) {
		const meta = this._map.get(schema);
		if (meta && typeof meta === "object" && "id" in meta) this._idmap.delete(meta.id);
		this._map.delete(schema);
		return this;
	}
	get(schema) {
		const p = schema._zod.parent;
		if (p) {
			const pm = { ...this.get(p) ?? {} };
			delete pm.id;
			const f = {
				...pm,
				...this._map.get(schema)
			};
			return Object.keys(f).length ? f : void 0;
		}
		return this._map.get(schema);
	}
	has(schema) {
		return this._map.has(schema);
	}
};
function registry() {
	return new $ZodRegistry();
}
(_a = globalThis).__zod_globalRegistry ?? (_a.__zod_globalRegistry = registry());
const globalRegistry = globalThis.__zod_globalRegistry;
//#endregion
//#region node_modules/.pnpm/zod@4.6.6/node_modules/zod/v4/core/api.js
function snapshotChecks(def) {
	if (def.checks) def.checks = [...def.checks];
	return def;
}
// @__NO_SIDE_EFFECTS__
function _string(Class, params) {
	return new Class(snapshotChecks({
		type: "string",
		...normalizeParams(params)
	}));
}
// @__NO_SIDE_EFFECTS__
function _email(Class, params) {
	return new Class({
		type: "string",
		format: "email",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _guid(Class, params) {
	return new Class({
		type: "string",
		format: "guid",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _uuid(Class, params) {
	return new Class({
		type: "string",
		format: "uuid",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _uuidv4(Class, params) {
	return new Class({
		type: "string",
		format: "uuid",
		check: "string_format",
		abort: false,
		version: "v4",
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _uuidv6(Class, params) {
	return new Class({
		type: "string",
		format: "uuid",
		check: "string_format",
		abort: false,
		version: "v6",
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _uuidv7(Class, params) {
	return new Class({
		type: "string",
		format: "uuid",
		check: "string_format",
		abort: false,
		version: "v7",
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _url(Class, params) {
	return new Class({
		type: "string",
		format: "url",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _emoji(Class, params) {
	return new Class({
		type: "string",
		format: "emoji",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _nanoid(Class, params) {
	return new Class({
		type: "string",
		format: "nanoid",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
/**
* @deprecated CUID v1 is deprecated by its authors due to information leakage
* (timestamps embedded in the id). Use {@link _cuid2} instead.
* See https://github.com/paralleldrive/cuid.
*/
// @__NO_SIDE_EFFECTS__
function _cuid(Class, params) {
	return new Class({
		type: "string",
		format: "cuid",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _cuid2(Class, params) {
	return new Class({
		type: "string",
		format: "cuid2",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _ulid(Class, params) {
	return new Class({
		type: "string",
		format: "ulid",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _xid(Class, params) {
	return new Class({
		type: "string",
		format: "xid",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _ksuid(Class, params) {
	return new Class({
		type: "string",
		format: "ksuid",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _ipv4(Class, params) {
	return new Class({
		type: "string",
		format: "ipv4",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _ipv6(Class, params) {
	return new Class({
		type: "string",
		format: "ipv6",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _cidrv4(Class, params) {
	return new Class({
		type: "string",
		format: "cidrv4",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _cidrv6(Class, params) {
	return new Class({
		type: "string",
		format: "cidrv6",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _base64(Class, params) {
	return new Class({
		type: "string",
		format: "base64",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _base64url(Class, params) {
	return new Class({
		type: "string",
		format: "base64url",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _e164(Class, params) {
	return new Class({
		type: "string",
		format: "e164",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _jwt(Class, params) {
	return new Class({
		type: "string",
		format: "jwt",
		check: "string_format",
		abort: false,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _isoDateTime(Class, params) {
	return new Class({
		type: "string",
		format: "datetime",
		check: "string_format",
		offset: false,
		local: false,
		precision: null,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _isoDate(Class, params) {
	return new Class({
		type: "string",
		format: "date",
		check: "string_format",
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _isoTime(Class, params) {
	return new Class({
		type: "string",
		format: "time",
		check: "string_format",
		precision: null,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _isoDuration(Class, params) {
	return new Class({
		type: "string",
		format: "duration",
		check: "string_format",
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _number(Class, params) {
	return new Class(snapshotChecks({
		type: "number",
		checks: [],
		...normalizeParams(params)
	}));
}
// @__NO_SIDE_EFFECTS__
function _int(Class, params) {
	return new Class({
		type: "number",
		check: "number_format",
		abort: false,
		format: "safeint",
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _boolean(Class, params) {
	return new Class({
		type: "boolean",
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _null$1(Class, params) {
	return new Class({
		type: "null",
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _unknown(Class) {
	return new Class({ type: "unknown" });
}
// @__NO_SIDE_EFFECTS__
function _never(Class, params) {
	return new Class({
		type: "never",
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _lt(value, params) {
	return new $ZodCheckLessThan({
		check: "less_than",
		...normalizeParams(params),
		value,
		inclusive: false
	});
}
// @__NO_SIDE_EFFECTS__
function _lte(value, params) {
	return new $ZodCheckLessThan({
		check: "less_than",
		...normalizeParams(params),
		value,
		inclusive: true
	});
}
// @__NO_SIDE_EFFECTS__
function _gt(value, params) {
	return new $ZodCheckGreaterThan({
		check: "greater_than",
		...normalizeParams(params),
		value,
		inclusive: false
	});
}
// @__NO_SIDE_EFFECTS__
function _gte(value, params) {
	return new $ZodCheckGreaterThan({
		check: "greater_than",
		...normalizeParams(params),
		value,
		inclusive: true
	});
}
// @__NO_SIDE_EFFECTS__
function _multipleOf(value, params) {
	return new $ZodCheckMultipleOf({
		check: "multiple_of",
		...normalizeParams(params),
		value
	});
}
// @__NO_SIDE_EFFECTS__
function _maxLength(maximum, params) {
	return new $ZodCheckMaxLength({
		check: "max_length",
		...normalizeParams(params),
		maximum
	});
}
// @__NO_SIDE_EFFECTS__
function _minLength(minimum, params) {
	return new $ZodCheckMinLength({
		check: "min_length",
		...normalizeParams(params),
		minimum
	});
}
// @__NO_SIDE_EFFECTS__
function _length(length, params) {
	return new $ZodCheckLengthEquals({
		check: "length_equals",
		...normalizeParams(params),
		length
	});
}
// @__NO_SIDE_EFFECTS__
function _regex(pattern, params) {
	return new $ZodCheckRegex({
		check: "string_format",
		format: "regex",
		...normalizeParams(params),
		pattern
	});
}
// @__NO_SIDE_EFFECTS__
function _lowercase(params) {
	return new $ZodCheckLowerCase({
		check: "string_format",
		format: "lowercase",
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _uppercase(params) {
	return new $ZodCheckUpperCase({
		check: "string_format",
		format: "uppercase",
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _includes(includes, params) {
	return new $ZodCheckIncludes({
		check: "string_format",
		format: "includes",
		...normalizeParams(params),
		includes
	});
}
// @__NO_SIDE_EFFECTS__
function _startsWith(prefix, params) {
	return new $ZodCheckStartsWith({
		check: "string_format",
		format: "starts_with",
		...normalizeParams(params),
		prefix
	});
}
// @__NO_SIDE_EFFECTS__
function _endsWith(suffix, params) {
	return new $ZodCheckEndsWith({
		check: "string_format",
		format: "ends_with",
		...normalizeParams(params),
		suffix
	});
}
// @__NO_SIDE_EFFECTS__
function _overwrite(tx) {
	return new $ZodCheckOverwrite({
		check: "overwrite",
		tx
	});
}
// @__NO_SIDE_EFFECTS__
function _normalize(form) {
	return /* @__PURE__ */ _overwrite((input) => input.normalize(form));
}
// @__NO_SIDE_EFFECTS__
function _trim() {
	return /* @__PURE__ */ _overwrite((input) => input.trim());
}
// @__NO_SIDE_EFFECTS__
function _toLowerCase() {
	return /* @__PURE__ */ _overwrite((input) => input.toLowerCase());
}
// @__NO_SIDE_EFFECTS__
function _toUpperCase() {
	return /* @__PURE__ */ _overwrite((input) => input.toUpperCase());
}
// @__NO_SIDE_EFFECTS__
function _slugify() {
	return /* @__PURE__ */ _overwrite((input) => slugify(input));
}
// @__NO_SIDE_EFFECTS__
function _array(Class, element, params) {
	return new Class({
		type: "array",
		element,
		...normalizeParams(params)
	});
}
// @__NO_SIDE_EFFECTS__
function _refine(Class, fn, _params) {
	return new Class({
		type: "custom",
		check: "custom",
		fn,
		...normalizeParams(_params)
	});
}
// @__NO_SIDE_EFFECTS__
function _superRefine(fn, params) {
	const ch = /* @__PURE__ */ _check((payload) => {
		payload.addIssue = (issue$2) => {
			if (typeof issue$2 === "string") payload.issues.push(issue(issue$2, payload.value, ch._zod.def));
			else {
				const _issue = issue$2;
				if (_issue.fatal) _issue.continue = false;
				_issue.code ?? (_issue.code = "custom");
				if (!("input" in _issue)) _issue.input = payload.value;
				_issue.inst ?? (_issue.inst = ch);
				_issue.continue ?? (_issue.continue = !ch._zod.def.abort);
				payload.issues.push(issue(_issue));
			}
		};
		return fn(payload.value, payload);
	}, params);
	return ch;
}
// @__NO_SIDE_EFFECTS__
function _check(fn, params) {
	const ch = new $ZodCheck({
		check: "custom",
		...normalizeParams(params)
	});
	ch._zod.check = fn;
	return ch;
}
//#endregion
//#region node_modules/.pnpm/zod@4.6.6/node_modules/zod/v4/core/to-json-schema.js
function assignProps(target, ...sources) {
	for (const source of sources) for (const key of Reflect.ownKeys(source)) if (Object.prototype.propertyIsEnumerable.call(source, key)) assignProp(target, key, source[key]);
	return target;
}
function initializeContext(params) {
	let target = params?.target ?? "draft-2020-12";
	if (target === "draft-4") target = "draft-04";
	if (target === "draft-7") target = "draft-07";
	return {
		processors: params.processors ?? {},
		metadataRegistry: params?.metadata ?? globalRegistry,
		target,
		unrepresentable: params?.unrepresentable ?? "throw",
		override: params?.override ?? (() => {}),
		io: params?.io ?? "output",
		counter: 0,
		seen: /* @__PURE__ */ new Map(),
		sharedDefsExtractedFor: void 0,
		sharedEmitDoneFor: void 0,
		cycles: params?.cycles ?? "ref",
		reused: params?.reused ?? "inline",
		intersections: [],
		deferred: [],
		external: params?.external ?? void 0
	};
}
/**
* Applies the `unrepresentable` setting at a site that has no JSON Schema equivalent. Throws
* `message` unless the setting (or the handler's return value) says otherwise. Returns `true` if a
* custom JSON Schema was written into `json`, in which case the caller must not write its own.
*/
function handleUnrepresentable(schema, ctx, json, params, message) {
	const result = typeof ctx.unrepresentable === "function" ? ctx.unrepresentable({
		zodSchema: schema,
		path: params.path,
		message
	}) : ctx.unrepresentable;
	if (result === "any") return false;
	if (result === void 0 || result === "throw") throw new Error(message);
	Object.assign(json, result);
	return true;
}
function processSchema(schema, ctx, _params = {
	path: [],
	schemaPath: []
}) {
	var _a;
	const def = schema._zod.def;
	const seen = ctx.seen.get(schema);
	if (seen) {
		seen.count++;
		if (_params.schemaPath.includes(schema)) seen.cycle = _params.path;
		return seen.schema;
	}
	const result = {
		schema: {},
		count: 1,
		cycle: void 0,
		path: _params.path
	};
	ctx.seen.set(schema, result);
	ctx.sharedDefsExtractedFor = void 0;
	ctx.sharedEmitDoneFor = void 0;
	const overrideSchema = schema._zod.toJSONSchema?.();
	if (overrideSchema) result.schema = overrideSchema;
	else {
		const params = {
			..._params,
			schemaPath: [..._params.schemaPath, schema],
			path: _params.path
		};
		if (schema._zod.processJSONSchema) schema._zod.processJSONSchema(ctx, result.schema, params);
		else {
			const _json = result.schema;
			const processor = ctx.processors[def.type];
			if (!processor) throw new Error(`[toJSONSchema]: Non-representable type encountered: ${def.type}`);
			processor(schema, ctx, _json, params);
		}
		const parent = schema._zod.parent;
		if (parent) {
			if (!result.ref) result.ref = parent;
			processSchema(parent, ctx, params);
			ctx.seen.get(parent).isParent = true;
		}
	}
	const meta = ctx.metadataRegistry.get(schema);
	if (meta) assignProps(result.schema, meta);
	if (ctx.io === "input" && isTransforming(schema)) {
		delete result.schema.examples;
		delete result.schema.default;
	}
	if (ctx.io === "input" && "_prefault" in result.schema) (_a = result.schema).default ?? (_a.default = result.schema._prefault);
	delete result.schema._prefault;
	return ctx.seen.get(schema).schema;
}
function encodeJSONPointerSegment(segment) {
	return segment.replace(/~/g, "~0").replace(/\//g, "~1");
}
function extractDefs(ctx, schema) {
	const root = ctx.seen.get(schema);
	if (!root) throw new Error("Unprocessed schema. This is a bug in Zod.");
	if (ctx.external && ctx.sharedDefsExtractedFor === ctx.external) return;
	const idToSchema = /* @__PURE__ */ new Map();
	for (const entry of ctx.seen.entries()) {
		const id = ctx.metadataRegistry.get(entry[0])?.id;
		if (id) {
			const existing = idToSchema.get(id);
			if (existing && existing !== entry[0]) throw new Error(`Duplicate schema id "${id}" detected during JSON Schema conversion. Two different schemas cannot share the same id when converted together.`);
			idToSchema.set(id, entry[0]);
		}
	}
	const makeURI = (entry) => {
		const defsSegment = ctx.target === "draft-2020-12" ? "$defs" : "definitions";
		if (ctx.external) {
			const externalId = ctx.external.registry.get(entry[0])?.id;
			const uriGenerator = ctx.external.uri ?? ((id) => id);
			if (externalId) return { ref: uriGenerator(externalId) };
			const id = entry[1].defId ?? entry[1].schema.id ?? `schema${ctx.counter++}`;
			entry[1].defId = id;
			return {
				defId: id,
				ref: `${uriGenerator("__shared")}#/${defsSegment}/${encodeJSONPointerSegment(id)}`
			};
		}
		const uriPrefix = `#`;
		const defUriPrefix = `${uriPrefix}/${defsSegment}/`;
		if (entry[1] === root && !entry[1].schema.id) return { ref: uriPrefix };
		const defId = entry[1].schema.id ?? `__schema${ctx.counter++}`;
		return {
			defId,
			ref: defUriPrefix + encodeJSONPointerSegment(defId)
		};
	};
	const extractToDef = (entry) => {
		if (entry[1].schema.$ref) return;
		const seen = entry[1];
		const { ref, defId } = makeURI(entry);
		seen.def = { ...seen.schema };
		if (defId) seen.defId = defId;
		const schema = seen.schema;
		for (const key in schema) delete schema[key];
		schema.$ref = ref;
	};
	if (ctx.cycles === "throw") for (const entry of ctx.seen.entries()) {
		const seen = entry[1];
		if (seen.cycle) throw new Error(`Cycle detected: #/${seen.cycle?.join("/")}/<root>

Set the \`cycles\` parameter to \`"ref"\` to resolve cyclical schemas with defs.`);
	}
	for (const entry of ctx.seen.entries()) {
		const seen = entry[1];
		if (schema === entry[0]) {
			extractToDef(entry);
			continue;
		}
		if (ctx.external) {
			const ext = ctx.external.registry.get(entry[0])?.id;
			if (schema !== entry[0] && ext) {
				extractToDef(entry);
				continue;
			}
		}
		if (ctx.metadataRegistry.get(entry[0])?.id) {
			extractToDef(entry);
			continue;
		}
		if (seen.cycle) {
			extractToDef(entry);
			continue;
		}
		if (seen.count > 1) {
			if (ctx.reused === "ref") extractToDef(entry);
		}
	}
	if (ctx.external) ctx.sharedDefsExtractedFor = ctx.external;
}
/** Rewrites `anyOf: [{type: "a"}, {type: "b"}]` to `type: ["a", "b"]`, which every JSON Schema draft treats as equivalent and most consumers render far better for the nullable case. Only branches that are a bare type assertion qualify — anything carrying a constraint, `$ref`, `const` or metadata is left alone. Runs after `flattenRef`, so a branch an override decorated or `$defs` extraction turned into a `$ref` is no longer bare and correctly stays in `anyOf`. `oneOf` is excluded: `integer` and `number` overlap, so "exactly one" and "at least one" are not the same there. OpenAPI 3.0 is excluded: its `type` must be a single string. */
function compactTypeUnion(schema) {
	const options = schema.anyOf;
	if (!Array.isArray(options) || options.length === 0 || schema.type !== void 0) return;
	const types = [];
	for (const option of options) {
		if (!option || typeof option !== "object") return;
		compactTypeUnion(option);
		const keys = Object.keys(option);
		if (keys.length !== 1 || keys[0] !== "type") return;
		const type = option.type;
		for (const member of Array.isArray(type) ? type : [type]) {
			if (typeof member !== "string") return;
			if (!types.includes(member)) types.push(member);
		}
	}
	delete schema.anyOf;
	schema.type = types.length === 1 ? types[0] : types;
}
/** Keywords `foldIntersection` knows how to combine. Anything else — `$ref`, `patternProperties`,
* an annotation like `description` — makes a member unfoldable, so a constraint this does not
* understand leaves the `allOf` alone instead of being silently dropped or misattributed. */
const FOLDABLE_KEYS = /* @__PURE__ */ new Set([
	"type",
	"properties",
	"required",
	"additionalProperties"
]);
const UNION_KEYS = ["oneOf", "anyOf"];
/** A member's constraint on a key it does not declare itself. A `catchall` states one; `false`, an absent `additionalProperties`, and the empty schema a loose object emits state nothing. */
function undeclaredConstraint(member) {
	const extra = member.additionalProperties;
	if (extra === void 0 || extra === false || typeof extra !== "object" || extra === null) return null;
	return Object.keys(extra).length ? extra : null;
}
/** Combines object members into the single object they describe together, or returns `null` if any of them carries a keyword outside {@link FOLDABLE_KEYS}. */
function foldObjects(members) {
	const objects = [];
	for (const member of members) {
		if (typeof member !== "object" || member.type !== "object") return null;
		for (const key in member) if (!FOLDABLE_KEYS.has(key)) return null;
		objects.push(member);
	}
	const properties = {};
	const required = /* @__PURE__ */ new Set();
	for (const object of objects) {
		for (const key in object.properties) {
			if (Object.prototype.hasOwnProperty.call(properties, key)) continue;
			const parts = [];
			for (const other of objects) {
				const part = other.properties?.[key] ?? undeclaredConstraint(other);
				if (part === null || part === void 0) continue;
				if (!parts.some((seen) => JSON.stringify(seen) === JSON.stringify(part))) parts.push(part);
			}
			assignProp(properties, key, parts.length === 1 ? parts[0] : foldObjects(parts) ?? { allOf: parts });
		}
		for (const key of object.required ?? []) required.add(key);
	}
	const folded = {
		type: "object",
		properties
	};
	if (required.size) folded.required = [...required];
	if (objects.every((object) => object.additionalProperties === false)) folded.additionalProperties = false;
	else {
		const constraints = [];
		for (const object of objects) {
			const constraint = undeclaredConstraint(object);
			if (constraint && !constraints.some((seen) => JSON.stringify(seen) === JSON.stringify(constraint))) constraints.push(constraint);
		}
		if (constraints.length === 1) folded.additionalProperties = constraints[0];
		else if (constraints.length > 1) folded.additionalProperties = { allOf: constraints };
	}
	return folded;
}
/** `additionalProperties` in an `allOf` member sees only that member's own `properties`, so two
* closed object members reject each other's keys and the schema validates nothing. Zod's parser
* pools the key sets instead — `handleIntersectionResults` reports a key as unrecognized only when
* *every* side rejects it — so the emitted schema has to pool them too, and folding the members
* into one object is the encoding that says so on every target.
*
* This runs from `finalize`, after `extractDefs`, which is what keeps it clear of the `$ref`
* machinery: a member extracted into `$defs` is already a `$ref` by now and declines to fold, so it
* keeps its reference and its own closedness rather than being inlined as a stale copy. */
function foldIntersection(json) {
	const allOf = json.allOf;
	if (!Array.isArray(allOf) || allOf.length < 2) return;
	for (const key of FOLDABLE_KEYS) if (key in json) return;
	const unions = allOf.filter((m) => UNION_KEYS.some((k) => Array.isArray(m[k])));
	let folded = null;
	if (!unions.length) folded = foldObjects(allOf);
	else {
		const union = unions[0];
		const keyword = UNION_KEYS.find((k) => Array.isArray(union[k]));
		if (Object.keys(union).length !== 1) return;
		const rest = allOf.filter((m) => m !== union);
		const branches = union[keyword].map((branch) => foldObjects([...rest, branch]));
		if (branches.some((b) => !b)) return;
		folded = { [keyword]: branches };
	}
	if (!folded) return;
	delete json.allOf;
	assignProps(json, folded);
}
function finalize(ctx, schema) {
	const root = ctx.seen.get(schema);
	if (!root) throw new Error("Unprocessed schema. This is a bug in Zod.");
	const flattenRef = (zodSchema) => {
		const seen = ctx.seen.get(zodSchema);
		if (seen.ref === null) return;
		const schema = seen.def ?? seen.schema;
		const _cached = { ...schema };
		const ref = seen.ref;
		seen.ref = null;
		if (ref) {
			flattenRef(ref);
			const refSeen = ctx.seen.get(ref);
			const refSchema = refSeen.schema;
			if (refSchema.$ref && (ctx.target === "draft-07" || ctx.target === "draft-04" || ctx.target === "openapi-3.0")) {
				schema.allOf = schema.allOf ?? [];
				schema.allOf.push(refSchema);
			} else assignProps(schema, refSchema);
			assignProps(schema, _cached);
			if (zodSchema._zod.parent === ref) for (const key in schema) {
				if (key === "$ref" || key === "allOf") continue;
				if (!(key in _cached)) delete schema[key];
			}
			if (refSchema.$ref && refSeen.def) for (const key in schema) {
				if (key === "$ref" || key === "allOf") continue;
				if (key in refSeen.def && JSON.stringify(schema[key]) === JSON.stringify(refSeen.def[key])) delete schema[key];
			}
		}
		const parent = zodSchema._zod.parent;
		if (parent && parent !== ref) {
			flattenRef(parent);
			const parentSeen = ctx.seen.get(parent);
			if (parentSeen?.schema.$ref) {
				schema.$ref = parentSeen.schema.$ref;
				if (parentSeen.def) for (const key in schema) {
					if (key === "$ref" || key === "allOf") continue;
					if (key in parentSeen.def && JSON.stringify(schema[key]) === JSON.stringify(parentSeen.def[key])) delete schema[key];
				}
			}
		}
		ctx.override({
			zodSchema,
			jsonSchema: schema,
			path: seen.path ?? []
		});
	};
	if (!ctx.external || ctx.sharedEmitDoneFor !== ctx.external) {
		for (const entry of [...ctx.seen.entries()].reverse()) flattenRef(entry[0]);
		if (ctx.target !== "openapi-3.0") for (const entry of ctx.seen.entries()) compactTypeUnion(entry[1].def ?? entry[1].schema);
		for (const rewrite of ctx.deferred) rewrite();
		if (ctx.intersections.length) {
			const carriers = /* @__PURE__ */ new Map();
			for (const seen of ctx.seen.values()) for (const json of [seen.schema, seen.def]) {
				const allOf = json?.allOf;
				if (!Array.isArray(allOf)) continue;
				const existing = carriers.get(allOf);
				if (existing) existing.push(json);
				else carriers.set(allOf, [json]);
			}
			for (const allOf of ctx.intersections) for (const json of carriers.get(allOf) ?? []) foldIntersection(json);
		}
	}
	const result = {};
	if (ctx.target === "draft-2020-12") result.$schema = "https://json-schema.org/draft/2020-12/schema";
	else if (ctx.target === "draft-07") result.$schema = "http://json-schema.org/draft-07/schema#";
	else if (ctx.target === "draft-04") result.$schema = "http://json-schema.org/draft-04/schema#";
	else if (ctx.target === "openapi-3.0") {}
	if (ctx.external?.uri) {
		const id = ctx.external.registry.get(schema)?.id;
		if (!id) throw new Error("Schema is missing an `id` property");
		result.$id = ctx.external.uri(id);
	}
	assignProps(result, root.defId ? root.schema : root.def ?? root.schema);
	const rootMetaId = ctx.metadataRegistry.get(schema)?.id;
	if (rootMetaId !== void 0 && result.id === rootMetaId) delete result.id;
	const defs = ctx.external?.defs ?? {};
	if (!ctx.external || ctx.sharedEmitDoneFor !== ctx.external) for (const entry of ctx.seen.entries()) {
		const seen = entry[1];
		if (seen.def && seen.defId) {
			if (seen.def.id === seen.defId) delete seen.def.id;
			assignProp(defs, seen.defId, seen.def);
		}
	}
	if (ctx.external) ctx.sharedEmitDoneFor = ctx.external;
	if (ctx.external) {} else if (Object.keys(defs).length > 0) {
		if (ctx.target === "draft-2020-12") result.$defs = defs;
		else result.definitions = defs;
	}
	try {
		const finalized = JSON.parse(JSON.stringify(result));
		Object.defineProperty(finalized, "~standard", {
			value: {
				...schema["~standard"],
				jsonSchema: {
					input: createStandardJSONSchemaMethod(schema, "input", ctx.processors),
					output: createStandardJSONSchemaMethod(schema, "output", ctx.processors)
				}
			},
			enumerable: false,
			writable: false
		});
		return finalized;
	} catch (_err) {
		throw new Error("Error converting schema to JSON.");
	}
}
function isTransforming(_schema, _ctx) {
	const ctx = _ctx ?? { seen: /* @__PURE__ */ new Set() };
	if (ctx.seen.has(_schema)) return false;
	ctx.seen.add(_schema);
	const def = _schema._zod.def;
	if (def.type === "transform") return true;
	if (def.type === "array") return isTransforming(def.element, ctx);
	if (def.type === "set") return isTransforming(def.valueType, ctx);
	if (def.type === "lazy") return isTransforming(def.getter(), ctx);
	if (def.type === "promise" || def.type === "optional" || def.type === "nonoptional" || def.type === "nullable" || def.type === "readonly" || def.type === "default" || def.type === "prefault" || def.type === "catch") return isTransforming(def.innerType, ctx);
	if (def.type === "intersection") return isTransforming(def.left, ctx) || isTransforming(def.right, ctx);
	if (def.type === "record" || def.type === "map") return isTransforming(def.keyType, ctx) || isTransforming(def.valueType, ctx);
	if (def.type === "pipe") {
		if (_schema._zod.traits.has("$ZodCodec")) return true;
		return isTransforming(def.in, ctx) || isTransforming(def.out, ctx);
	}
	if (def.type === "object") {
		for (const key in def.shape) if (isTransforming(def.shape[key], ctx)) return true;
		return false;
	}
	if (def.type === "union") {
		for (const option of def.options) if (isTransforming(option, ctx)) return true;
		return false;
	}
	if (def.type === "tuple") {
		for (const item of def.items) if (isTransforming(item, ctx)) return true;
		if (def.rest && isTransforming(def.rest, ctx)) return true;
		return false;
	}
	return false;
}
/**
* Creates a toJSONSchema method for a schema instance.
* This encapsulates the logic of initializing context, processing, extracting defs, and finalizing.
*/
const createToJSONSchemaMethod = (schema, processors = {}) => (params) => {
	const ctx = initializeContext({
		...params,
		processors
	});
	processSchema(schema, ctx);
	extractDefs(ctx, schema);
	return finalize(ctx, schema);
};
const createStandardJSONSchemaMethod = (schema, io, processors = {}) => (params) => {
	const { libraryOptions, target } = params ?? {};
	const ctx = initializeContext({
		...libraryOptions ?? {},
		target,
		io,
		processors
	});
	processSchema(schema, ctx);
	extractDefs(ctx, schema);
	return finalize(ctx, schema);
};
//#endregion
//#region node_modules/.pnpm/zod@4.6.6/node_modules/zod/v4/core/json-schema-processors.js
const narrowMin = (agg, key, value) => {
	if (agg[key] === void 0 || value > agg[key]) agg[key] = value;
};
const narrowMax = (agg, key, value) => {
	if (agg[key] === void 0 || value < agg[key]) agg[key] = value;
};
const narrowBoth = (agg, value) => {
	narrowMin(agg, "minimum", value);
	narrowMax(agg, "maximum", value);
};
const addDivisor = (agg, value) => {
	agg.multipleOf ?? (agg.multipleOf = []);
	if (!agg.multipleOf.includes(value)) agg.multipleOf.push(value);
};
const addPattern = (agg, pattern) => {
	agg.patterns ?? (agg.patterns = /* @__PURE__ */ new Set());
	agg.patterns.add(pattern);
};
const intersectMime = (agg, mime) => {
	agg.mime = agg.mime ? agg.mime.filter((m) => mime.includes(m)) : [...mime];
};
const setFormat = (agg, format) => {
	agg.format = format;
	if (format.includes("int")) agg.isInt = true;
};
const minContributor = (agg, def) => narrowMin(agg, "minimum", def.minimum);
const maxContributor = (agg, def) => narrowMax(agg, "maximum", def.maximum);
const formatContributor = (ranges) => (agg, def) => {
	setFormat(agg, def.format);
	const [minimum, maximum] = ranges[def.format];
	narrowMin(agg, "minimum", minimum);
	narrowMax(agg, "maximum", maximum);
};
const contributors = {
	greater_than: (agg, def) => narrowMin(agg, def.inclusive ? "minimum" : "exclusiveMinimum", def.value),
	less_than: (agg, def) => narrowMax(agg, def.inclusive ? "maximum" : "exclusiveMaximum", def.value),
	multiple_of: (agg, def) => addDivisor(agg, def.value),
	number_format: formatContributor(NUMBER_FORMAT_RANGES),
	bigint_format: formatContributor(BIGINT_FORMAT_RANGES),
	min_length: minContributor,
	max_length: maxContributor,
	length_equals: (agg, def) => narrowBoth(agg, def.length),
	min_size: minContributor,
	max_size: maxContributor,
	size_equals: (agg, def) => narrowBoth(agg, def.size),
	string_format: (agg, def) => {
		setFormat(agg, def.format);
		if (def.pattern) addPattern(agg, def.pattern);
		if (def.format === "base64" || def.format === "base64url") agg.contentEncoding = def.format;
		if (def.local || def.precision === -1) agg.laxFormat = true;
	},
	mime_type: (agg, def) => intersectMime(agg, def.mime)
};
function aggregateChecks(schema) {
	const agg = {};
	const def = schema._zod.def;
	const list = schema._zod.traits.has("$ZodCheck") ? [schema, ...def.checks ?? []] : def.checks ?? [];
	for (const ch of list) contributors[ch._zod.def.check]?.(agg, ch._zod.def);
	const bag = schema._zod.bag;
	if (bag.minimum !== void 0) narrowMin(agg, "minimum", bag.minimum);
	if (bag.exclusiveMinimum !== void 0) narrowMin(agg, "exclusiveMinimum", bag.exclusiveMinimum);
	if (bag.maximum !== void 0) narrowMax(agg, "maximum", bag.maximum);
	if (bag.exclusiveMaximum !== void 0) narrowMax(agg, "exclusiveMaximum", bag.exclusiveMaximum);
	if (bag.multipleOf !== void 0) addDivisor(agg, bag.multipleOf);
	if (bag.format !== void 0) {
		agg.format ?? (agg.format = bag.format);
		if (bag.format.includes("int")) agg.isInt = true;
	}
	if (bag.mime) intersectMime(agg, bag.mime);
	for (const pattern of bag.patterns ?? []) addPattern(agg, pattern);
	return agg;
}
const formatMap = {
	guid: "uuid",
	url: "uri",
	datetime: "date-time",
	json_string: "json-string",
	regex: ""
};
const exactPatterns = /* @__PURE__ */ new Map([[base64Charset, base64$1], [base64urlCharset, base64url]]);
const exactPattern = (p) => exactPatterns.get(p) ?? p;
const stringProcessor = (schema, ctx, _json, _params) => {
	const json = _json;
	json.type = "string";
	const { minimum, maximum, format, patterns, contentEncoding, laxFormat } = aggregateChecks(schema);
	if (typeof minimum === "number") json.minLength = minimum;
	if (typeof maximum === "number") json.maxLength = maximum;
	if (format) {
		json.format = formatMap[format] ?? format;
		if (json.format === "") delete json.format;
		if (format === "time" || laxFormat) delete json.format;
	}
	if (contentEncoding) json.contentEncoding = contentEncoding;
	if (patterns && patterns.size > 0) {
		const patternList = [...patterns].map(exactPattern);
		if (patternList.length === 1) json.pattern = patternList[0].source;
		else if (patternList.length > 1) json.allOf = [...patternList.map((regex) => ({
			...ctx.target === "draft-07" || ctx.target === "draft-04" || ctx.target === "openapi-3.0" ? { type: "string" } : {},
			pattern: regex.source
		}))];
	}
};
const numberProcessor = (schema, ctx, _json, params) => {
	const json = _json;
	const { minimum, maximum, multipleOf, exclusiveMaximum, exclusiveMinimum, isInt } = aggregateChecks(schema);
	json.type = isInt ? "integer" : "number";
	const exMin = typeof exclusiveMinimum === "number" && exclusiveMinimum >= (minimum ?? Number.NEGATIVE_INFINITY);
	const exMax = typeof exclusiveMaximum === "number" && exclusiveMaximum <= (maximum ?? Number.POSITIVE_INFINITY);
	const legacy = ctx.target === "draft-04" || ctx.target === "openapi-3.0";
	if (exMin) {
		if (legacy) {
			json.minimum = exclusiveMinimum;
			json.exclusiveMinimum = true;
		} else json.exclusiveMinimum = exclusiveMinimum;
	} else if (typeof minimum === "number") json.minimum = minimum;
	if (exMax) {
		if (legacy) {
			json.maximum = exclusiveMaximum;
			json.exclusiveMaximum = true;
		} else json.exclusiveMaximum = exclusiveMaximum;
	} else if (typeof maximum === "number") json.maximum = maximum;
	if (multipleOf) {
		const divisors = /* @__PURE__ */ new Set();
		for (const divisor of multipleOf) if (Number.isFinite(divisor) && divisor !== 0) divisors.add(Math.abs(divisor));
		else handleUnrepresentable(schema, ctx, json, params, `A multipleOf divisor of ${divisor} cannot be represented in JSON Schema`);
		const [first, ...rest] = divisors;
		if (first !== void 0) json.multipleOf = first;
		if (rest.length) json.allOf = [...json.allOf ?? [], ...rest.map((m) => ({ multipleOf: m }))];
	}
};
const booleanProcessor = (_schema, _ctx, json, _params) => {
	json.type = "boolean";
};
const nullProcessor = (_schema, ctx, json, _params) => {
	if (ctx.target === "openapi-3.0") {
		json.type = "string";
		json.nullable = true;
		json.enum = [null];
	} else json.type = "null";
};
const neverProcessor = (_schema, _ctx, json, _params) => {
	json.not = {};
};
const enumProcessor = (schema, _ctx, json, _params) => {
	const def = schema._zod.def;
	const values = getEnumValues(def.entries);
	if (values.length === 0) {
		json.not = {};
		return;
	}
	if (values.every((v) => typeof v === "number")) json.type = "number";
	if (values.every((v) => typeof v === "string")) json.type = "string";
	json.enum = values;
};
const literalProcessor = (schema, ctx, json, params) => {
	const def = schema._zod.def;
	if (def.values.length === 0) {
		json.not = {};
		return;
	}
	const vals = [];
	for (const val of def.values) if (val === void 0) {
		if (handleUnrepresentable(schema, ctx, json, params, "Literal `undefined` cannot be represented in JSON Schema")) return;
	} else if (typeof val === "bigint") {
		if (handleUnrepresentable(schema, ctx, json, params, "BigInt literals cannot be represented in JSON Schema")) return;
		vals.push(Number(val));
	} else vals.push(val);
	if (vals.length === 0) {} else if (vals.length === 1) {
		const val = vals[0];
		json.type = val === null ? "null" : typeof val;
		if (ctx.target === "draft-04" || ctx.target === "openapi-3.0") json.enum = [val];
		else json.const = val;
	} else {
		if (vals.every((v) => typeof v === "number")) json.type = "number";
		if (vals.every((v) => typeof v === "string")) json.type = "string";
		if (vals.every((v) => typeof v === "boolean")) json.type = "boolean";
		if (vals.every((v) => v === null)) json.type = "null";
		json.enum = vals;
	}
};
const customProcessor = (schema, ctx, json, params) => {
	handleUnrepresentable(schema, ctx, json, params, "Custom types cannot be represented in JSON Schema");
};
const transformProcessor = (schema, ctx, json, params) => {
	handleUnrepresentable(schema, ctx, json, params, "Transforms cannot be represented in JSON Schema");
};
const arrayProcessor = (schema, ctx, _json, params) => {
	const json = _json;
	const def = schema._zod.def;
	const { minimum, maximum } = aggregateChecks(schema);
	if (typeof minimum === "number") json.minItems = minimum;
	if (typeof maximum === "number") json.maxItems = maximum;
	json.type = "array";
	json.items = processSchema(def.element, ctx, {
		...params,
		path: [...params.path, "items"]
	});
};
function inputOptin(schema) {
	const def = schema._zod.def;
	if (def.type === "pipe" && def.in._zod.traits.has("$ZodTransform")) return inputOptin(def.out);
	if (def.type === "catch") return inputOptin(def.innerType);
	return schema._zod.optin;
}
const objectProcessor = (schema, ctx, _json, params) => {
	const json = _json;
	const def = schema._zod.def;
	const shape = def.shape;
	if (Object.getOwnPropertySymbols(shape).length && handleUnrepresentable(schema, ctx, json, params, "Symbol keys cannot be represented in JSON Schema")) return;
	json.type = "object";
	json.properties = {};
	for (const key in shape) assignProp(json.properties, key, processSchema(shape[key], ctx, {
		...params,
		path: [
			...params.path,
			"properties",
			key
		]
	}));
	const requiredKeys = [];
	for (const key of Object.keys(shape)) {
		const field = def.shape[key];
		if (ctx.io === "input" ? inputOptin(field) === void 0 : field._zod.optout === void 0) requiredKeys.push(key);
	}
	if (requiredKeys.length > 0) json.required = requiredKeys;
	if (def.catchall?._zod.def.type === "never") json.additionalProperties = false;
	else if (!def.catchall) {
		if (ctx.io === "output") json.additionalProperties = false;
	} else if (def.catchall) json.additionalProperties = processSchema(def.catchall, ctx, {
		...params,
		path: [...params.path, "additionalProperties"]
	});
};
const unionProcessor = (schema, ctx, json, params) => {
	const def = schema._zod.def;
	const isExclusive = def.inclusive === false;
	const options = def.options.map((x, i) => processSchema(x, ctx, {
		...params,
		path: [
			...params.path,
			isExclusive ? "oneOf" : "anyOf",
			i
		]
	}));
	if (isExclusive) json.oneOf = options;
	else json.anyOf = options;
};
const intersectionProcessor = (schema, ctx, json, params) => {
	const def = schema._zod.def;
	const a = processSchema(def.left, ctx, {
		...params,
		path: [
			...params.path,
			"allOf",
			0
		]
	});
	const b = processSchema(def.right, ctx, {
		...params,
		path: [
			...params.path,
			"allOf",
			1
		]
	});
	const isSimpleIntersection = (val) => "allOf" in val && Object.keys(val).length === 1;
	const allOf = [...isSimpleIntersection(a) ? a.allOf : [a], ...isSimpleIntersection(b) ? b.allOf : [b]];
	json.allOf = allOf;
	ctx.intersections.push(allOf);
};
/** JSON object keys are always strings, so a numeric record key schema is re-expressed over the
* numeric-string form the record parser matches. Deferred to `finalize`, after the flatten: a key
* behind a wrapper only carries its own `type` before then, and a union key only has its branches.
*
* A numeric bound cannot apply to a property name, so `minimum` and its siblings are dropped rather
* than carried over: keeping them beside `type: "string"` reproduces the match-nothing schema this
* exists to fix. A key that carries one therefore emits wider than the record parses — `z.record(z.number().min(5), V)`
* accepts `"3"` — which is the deliberate trade, since throwing on it would reject an ordinary schema
* outright. */
function stringifyKeyNames(bySchema, json, visited) {
	if (json.$ref) {
		if (visited.has(json)) return json;
		visited.add(json);
		const def = bySchema.get(json)?.def;
		if (!def) return json;
		const inlined = stringifyKeyNames(bySchema, def, visited);
		return inlined === def ? json : inlined;
	}
	for (const keyword of ["anyOf", "oneOf"]) {
		const branches = json[keyword];
		if (!Array.isArray(branches)) continue;
		const mapped = branches.map((branch) => stringifyKeyNames(bySchema, branch, visited));
		if (mapped.some((branch, i) => branch !== branches[i])) json = {
			...json,
			[keyword]: mapped
		};
	}
	const types = Array.isArray(json.type) ? json.type : [json.type];
	const numericType = !types.includes("string") && types.some((t) => t === "number" || t === "integer");
	const values = json.enum ?? (json.const !== void 0 ? [json.const] : void 0);
	if (!numericType && !values?.some((v) => typeof v === "number")) return json;
	const { minimum, maximum, exclusiveMinimum, exclusiveMaximum, multipleOf, format, id, ...rest } = json;
	if (rest.enum) rest.enum = rest.enum.map((v) => typeof v === "number" ? String(v) : v);
	else if (typeof rest.const === "number") rest.const = String(rest.const);
	if (!numericType) return rest;
	rest.type = "string";
	if (!values) rest.pattern = (types.includes("number") ? number$1 : integer).source;
	return rest;
}
/** Every record of one conversion, so the carriers are found in a single pass rather than once per record. */
const pendingRecords = /* @__PURE__ */ new WeakMap();
function rewriteKeyNames(ctx) {
	const bySchema = /* @__PURE__ */ new Map();
	for (const entry of ctx.seen.values()) if (entry.def && !bySchema.has(entry.schema)) bySchema.set(entry.schema, entry);
	const rewrites = /* @__PURE__ */ new Map();
	for (const record of pendingRecords.get(ctx) ?? []) {
		const seen = ctx.seen.get(record);
		const names = (seen?.def ?? seen?.schema)?.propertyNames;
		if (!names || names === true || rewrites.has(names)) continue;
		const rewritten = stringifyKeyNames(bySchema, names, /* @__PURE__ */ new Set());
		if (rewritten !== names) rewrites.set(names, rewritten);
	}
	if (!rewrites.size) return;
	for (const entry of ctx.seen.values()) for (const carrier of [entry.schema, entry.def]) {
		const rewritten = carrier && rewrites.get(carrier.propertyNames);
		if (rewritten) carrier.propertyNames = rewritten;
	}
}
const recordProcessor = (schema, ctx, _json, params) => {
	const json = _json;
	const def = schema._zod.def;
	json.type = "object";
	const keyType = def.keyType;
	const patterns = aggregateChecks(keyType).patterns;
	if (def.mode === "loose" && patterns && patterns.size > 0) {
		const valueSchema = processSchema(def.valueType, ctx, {
			...params,
			path: [
				...params.path,
				"patternProperties",
				"*"
			]
		});
		json.patternProperties = {};
		for (const pattern of patterns) assignProp(json.patternProperties, exactPattern(pattern).source, valueSchema);
	} else {
		if (ctx.target === "draft-07" || ctx.target === "draft-2020-12") {
			json.propertyNames = processSchema(def.keyType, ctx, {
				...params,
				path: [...params.path, "propertyNames"]
			});
			let pending = pendingRecords.get(ctx);
			if (!pending) {
				pending = [];
				pendingRecords.set(ctx, pending);
				ctx.deferred.push(() => rewriteKeyNames(ctx));
			}
			pending.push(schema);
		}
		json.additionalProperties = processSchema(def.valueType, ctx, {
			...params,
			path: [...params.path, "additionalProperties"]
		});
	}
	const keyValues = keyType._zod.values;
	const omittableOnInput = ctx.io === "input" && inputOptin(def.valueType) !== void 0;
	if (keyValues && !def.partial && !omittableOnInput) {
		const validKeyValues = [...keyValues].filter((v) => typeof v === "string" || typeof v === "number");
		if (validKeyValues.length > 0) json.required = validKeyValues.map(String);
	}
};
const nullableProcessor = (schema, ctx, json, params) => {
	const def = schema._zod.def;
	const inner = processSchema(def.innerType, ctx, params);
	const seen = ctx.seen.get(schema);
	if (ctx.target === "openapi-3.0") {
		seen.ref = def.innerType;
		json.nullable = true;
	} else json.anyOf = [inner, { type: "null" }];
};
const nonoptionalProcessor = (schema, ctx, _json, params) => {
	const def = schema._zod.def;
	processSchema(def.innerType, ctx, params);
	const seen = ctx.seen.get(schema);
	seen.ref = def.innerType;
};
/** Round-trips a default value through JSON so the emitted schema is guaranteed to be valid JSON.
* A BigInt has no reliable encoding, so it goes through `unrepresentable` like any other
* unrepresentable value. Returns a sentinel when the caller must not write a default of its own. */
const UNREPRESENTABLE_DEFAULT = Symbol();
function serializeDefaultValue(value, schema, ctx, json, params) {
	let unrepresentable = false;
	const serialized = JSON.stringify(value, (_, val) => {
		if (typeof val !== "bigint") return val;
		unrepresentable = true;
		return null;
	});
	if (!unrepresentable) return JSON.parse(serialized);
	handleUnrepresentable(schema, ctx, json, params, "BigInt defaults cannot be represented in JSON Schema");
	return UNREPRESENTABLE_DEFAULT;
}
const defaultProcessor = (schema, ctx, json, params) => {
	const def = schema._zod.def;
	processSchema(def.innerType, ctx, params);
	const seen = ctx.seen.get(schema);
	seen.ref = def.innerType;
	const value = serializeDefaultValue(def.defaultValue, schema, ctx, json, params);
	if (value !== UNREPRESENTABLE_DEFAULT) json.default = value;
};
const prefaultProcessor = (schema, ctx, json, params) => {
	const def = schema._zod.def;
	processSchema(def.innerType, ctx, params);
	const seen = ctx.seen.get(schema);
	seen.ref = def.innerType;
	if (ctx.io !== "input") return;
	const value = serializeDefaultValue(def.defaultValue, schema, ctx, json, params);
	if (value !== UNREPRESENTABLE_DEFAULT) json._prefault = value;
};
const catchProcessor = (schema, ctx, json, params) => {
	const def = schema._zod.def;
	processSchema(def.innerType, ctx, params);
	const seen = ctx.seen.get(schema);
	seen.ref = def.innerType;
	let catchValue;
	try {
		catchValue = def.catchValue(void 0);
	} catch {
		handleUnrepresentable(schema, ctx, json, params, "Dynamic catch values are not supported in JSON Schema");
		return;
	}
	json.default = catchValue;
};
const pipeProcessor = (schema, ctx, _json, params) => {
	const def = schema._zod.def;
	const inIsTransform = def.in._zod.traits.has("$ZodTransform");
	const innerType = ctx.io === "input" ? inIsTransform ? def.out : def.in : def.out;
	processSchema(innerType, ctx, params);
	const seen = ctx.seen.get(schema);
	seen.ref = innerType;
};
const readonlyProcessor = (schema, ctx, json, params) => {
	const def = schema._zod.def;
	processSchema(def.innerType, ctx, params);
	const seen = ctx.seen.get(schema);
	seen.ref = def.innerType;
	json.readOnly = true;
};
const optionalProcessor = (schema, ctx, _json, params) => {
	const def = schema._zod.def;
	processSchema(def.innerType, ctx, params);
	const seen = ctx.seen.get(schema);
	seen.ref = def.innerType;
};
//#endregion
//#region node_modules/.pnpm/zod@4.6.6/node_modules/zod/v4/classic/errors.js
const _installedErrorProtos = /* @__PURE__ */ new WeakSet([Object.prototype, Error.prototype]);
function _lazyMethod(proto, key, make) {
	Object.defineProperty(proto, key, {
		configurable: true,
		enumerable: false,
		get() {
			const value = make(this);
			Object.defineProperty(this, key, {
				value,
				configurable: true,
				writable: true
			});
			return value;
		},
		set(value) {
			Object.defineProperty(this, key, {
				value,
				configurable: true,
				writable: true
			});
		}
	});
}
const initializer = (inst, issues) => {
	$ZodError.init(inst, issues);
	inst.name = "ZodError";
	const proto = Object.getPrototypeOf(inst);
	if (_installedErrorProtos.has(proto)) return;
	_installedErrorProtos.add(proto);
	_lazyMethod(proto, "format", (self) => (mapper) => formatError(self, mapper));
	_lazyMethod(proto, "flatten", (self) => (mapper) => flattenError(self, mapper));
	_lazyMethod(proto, "addIssue", (self) => (issue) => {
		self.issues.push(issue);
		self.message = JSON.stringify(self.issues, jsonStringifyReplacer, 2);
	});
	_lazyMethod(proto, "addIssues", (self) => (issues) => {
		self.issues.push(...issues);
		self.message = JSON.stringify(self.issues, jsonStringifyReplacer, 2);
	});
	Object.defineProperty(proto, "isEmpty", {
		configurable: true,
		enumerable: false,
		get() {
			return this.issues.length === 0;
		}
	});
};
const ZodRealError = /*@__PURE__*/ $constructor("ZodError", initializer, void 0, { Parent: Error });
//#endregion
//#region node_modules/.pnpm/zod@4.6.6/node_modules/zod/v4/classic/parse.js
const parse = /* @__PURE__ */ _parse(ZodRealError);
const parseAsync = /* @__PURE__ */ _parseAsync(ZodRealError);
const safeParse = /* @__PURE__ */ _safeParse(ZodRealError);
const safeParseAsync = /* @__PURE__ */ _safeParseAsync(ZodRealError);
const encode = /* @__PURE__ */ _encode(ZodRealError);
const decode = /* @__PURE__ */ _decode(ZodRealError);
const encodeAsync = /* @__PURE__ */ _encodeAsync(ZodRealError);
const decodeAsync = /* @__PURE__ */ _decodeAsync(ZodRealError);
const safeEncode = /* @__PURE__ */ _safeEncode(ZodRealError);
const safeDecode = /* @__PURE__ */ _safeDecode(ZodRealError);
const safeEncodeAsync = /* @__PURE__ */ _safeEncodeAsync(ZodRealError);
const safeDecodeAsync = /* @__PURE__ */ _safeDecodeAsync(ZodRealError);
//#endregion
//#region node_modules/.pnpm/zod@4.6.6/node_modules/zod/v4/classic/schemas.js
function _ensureDefaultLocale() {
	if (!globalConfig.localeError) config(en_default());
}
function _ensureDefaultMemoizer() {
	if (!globalConfig.memoizer) config({ memoizer: memoizer() });
}
const ZodType = /*@__PURE__*/ $constructor("ZodType", (inst, def) => {
	_ensureDefaultLocale();
	$ZodType.init(inst, def);
	inst.def = def;
	inst.type = def.type;
	return inst;
}, {
	check(...chks) {
		const def = this.def;
		return this.clone(mergeDefs(def, { checks: [...def.checks ?? [], ...chks.map((ch) => typeof ch === "function" ? { _zod: {
			check: ch,
			def: { check: "custom" },
			onattach: []
		} } : ch)] }), { parent: true });
	},
	with(...chks) {
		return this.check(...chks);
	},
	clone(def, params) {
		return clone(this, def, params);
	},
	brand() {
		return this;
	},
	register(reg, meta) {
		reg.add(this, meta);
		return this;
	},
	refine(check, params) {
		return this.check(refine(check, params));
	},
	superRefine(refinement, params) {
		return this.check(superRefine(refinement, params));
	},
	overwrite(fn) {
		return this.check(/* @__PURE__ */ _overwrite(fn));
	},
	optional() {
		return optional(this);
	},
	exactOptional() {
		return exactOptional(this);
	},
	nullable() {
		return nullable(this);
	},
	nullish() {
		return optional(nullable(this));
	},
	nonoptional(params) {
		return nonoptional(this, params);
	},
	array() {
		return array(this);
	},
	or(arg) {
		return union([this, arg]);
	},
	and(arg) {
		return intersection(this, arg);
	},
	transform(tx) {
		return pipe(this, transform(tx));
	},
	default(d) {
		return _default(this, d);
	},
	prefault(d) {
		return prefault(this, d);
	},
	catch(params) {
		return _catch(this, params);
	},
	pipe(target) {
		return pipe(this, target);
	},
	readonly() {
		return readonly(this);
	},
	describe(description) {
		const cl = this.clone();
		globalRegistry.add(cl, { description });
		return cl;
	},
	meta(...args) {
		if (args.length === 0) return globalRegistry.get(this);
		const cl = this.clone();
		globalRegistry.add(cl, args[0]);
		return cl;
	},
	isOptional() {
		return this.safeParse(void 0).success;
	},
	isNullable() {
		return this.safeParse(null).success;
	},
	apply(fn, ...args) {
		return args.length === 0 ? fn(this) : fn(this, ...args);
	},
	get "~standard"() {
		return hide(this, "~standard", {
			...standardProps(this),
			jsonSchema: {
				input: createStandardJSONSchemaMethod(this, "input"),
				output: createStandardJSONSchemaMethod(this, "output")
			}
		});
	},
	set "~standard"(value) {
		own(this, "~standard", value);
	},
	parse: function _parse(data, params) {
		return parse(this, data, params, { callee: _parse });
	},
	parseAsync: async function _parseAsync(data, params) {
		return await parseAsync(this, data, params, { callee: _parseAsync });
	},
	safeParse(data, params) {
		return safeParse(this, data, params);
	},
	async safeParseAsync(data, params) {
		return safeParseAsync(this, data, params);
	},
	get spa() {
		return this?.safeParseAsync;
	},
	set spa(value) {
		own(this, "spa", value);
	},
	validate(data, params) {
		return validate(this, data, params);
	},
	validateAsync(data, params) {
		return validateAsync$1(this, data, params);
	},
	encode: function _encode(data, params) {
		return encode(this, data, params, { callee: _encode });
	},
	decode: function _decode(data, params) {
		return decode(this, data, params, { callee: _decode });
	},
	encodeAsync: async function _encodeAsync(data, params) {
		return await encodeAsync(this, data, params, { callee: _encodeAsync });
	},
	decodeAsync: async function _decodeAsync(data, params) {
		return await decodeAsync(this, data, params, { callee: _decodeAsync });
	},
	safeEncode(data, params) {
		return safeEncode(this, data, params);
	},
	safeDecode(data, params) {
		return safeDecode(this, data, params);
	},
	async safeEncodeAsync(data, params) {
		return safeEncodeAsync(this, data, params);
	},
	async safeDecodeAsync(data, params) {
		return safeDecodeAsync(this, data, params);
	},
	toJSONSchema(params) {
		return createToJSONSchemaMethod(this, {})(params);
	},
	get description() {
		return globalRegistry.get(this)?.description;
	},
	get _def() {
		return this._zod.def;
	}
});
/** @internal */
const _ZodString = /*@__PURE__*/ $constructor("_ZodString", (inst, def) => {
	$ZodString.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => stringProcessor(inst, ctx, json, params);
}, /*@__PURE__*/ derived({
	format: (inst) => aggregateChecks(inst).format ?? null,
	minLength: (inst) => aggregateChecks(inst).minimum ?? null,
	maxLength: (inst) => aggregateChecks(inst).maximum ?? null
}, {
	regex(...args) {
		return this.check(/* @__PURE__ */ _regex(...args));
	},
	includes(...args) {
		return this.check(/* @__PURE__ */ _includes(...args));
	},
	startsWith(...args) {
		return this.check(/* @__PURE__ */ _startsWith(...args));
	},
	endsWith(...args) {
		return this.check(/* @__PURE__ */ _endsWith(...args));
	},
	min(...args) {
		return this.check(/* @__PURE__ */ _minLength(...args));
	},
	max(...args) {
		return this.check(/* @__PURE__ */ _maxLength(...args));
	},
	length(...args) {
		return this.check(/* @__PURE__ */ _length(...args));
	},
	nonempty(...args) {
		return this.check(/* @__PURE__ */ _minLength(1, ...args));
	},
	lowercase(params) {
		return this.check(/* @__PURE__ */ _lowercase(params));
	},
	uppercase(params) {
		return this.check(/* @__PURE__ */ _uppercase(params));
	},
	trim() {
		return this.check(/* @__PURE__ */ _trim());
	},
	normalize(...args) {
		return this.check(/* @__PURE__ */ _normalize(...args));
	},
	toLowerCase() {
		return this.check(/* @__PURE__ */ _toLowerCase());
	},
	toUpperCase() {
		return this.check(/* @__PURE__ */ _toUpperCase());
	},
	slugify() {
		return this.check(/* @__PURE__ */ _slugify());
	}
}));
const ZodString = /*@__PURE__*/ $constructor("ZodString", (inst, def) => {
	$ZodString.init(inst, def);
	_ZodString.init(inst, def);
}, {
	email(params) {
		return this.check(/* @__PURE__ */ _email(ZodEmail, params));
	},
	url(params) {
		return this.check(/* @__PURE__ */ _url(ZodURL, params));
	},
	jwt(params) {
		return this.check(/* @__PURE__ */ _jwt(ZodJWT, params));
	},
	emoji(params) {
		return this.check(/* @__PURE__ */ _emoji(ZodEmoji, params));
	},
	guid(params) {
		return this.check(/* @__PURE__ */ _guid(ZodGUID, params));
	},
	uuid(params) {
		return this.check(/* @__PURE__ */ _uuid(ZodUUID, params));
	},
	uuidv4(params) {
		return this.check(/* @__PURE__ */ _uuidv4(ZodUUID, params));
	},
	uuidv6(params) {
		return this.check(/* @__PURE__ */ _uuidv6(ZodUUID, params));
	},
	uuidv7(params) {
		return this.check(/* @__PURE__ */ _uuidv7(ZodUUID, params));
	},
	nanoid(params) {
		return this.check(/* @__PURE__ */ _nanoid(ZodNanoID, params));
	},
	cuid(params) {
		return this.check(/* @__PURE__ */ _cuid(ZodCUID, params));
	},
	cuid2(params) {
		return this.check(/* @__PURE__ */ _cuid2(ZodCUID2, params));
	},
	ulid(params) {
		return this.check(/* @__PURE__ */ _ulid(ZodULID, params));
	},
	base64(params) {
		return this.check(/* @__PURE__ */ _base64(ZodBase64, params));
	},
	base64url(params) {
		return this.check(/* @__PURE__ */ _base64url(ZodBase64URL, params));
	},
	xid(params) {
		return this.check(/* @__PURE__ */ _xid(ZodXID, params));
	},
	ksuid(params) {
		return this.check(/* @__PURE__ */ _ksuid(ZodKSUID, params));
	},
	ipv4(params) {
		return this.check(/* @__PURE__ */ _ipv4(ZodIPv4, params));
	},
	ipv6(params) {
		return this.check(/* @__PURE__ */ _ipv6(ZodIPv6, params));
	},
	cidrv4(params) {
		return this.check(/* @__PURE__ */ _cidrv4(ZodCIDRv4, params));
	},
	cidrv6(params) {
		return this.check(/* @__PURE__ */ _cidrv6(ZodCIDRv6, params));
	},
	e164(params) {
		return this.check(/* @__PURE__ */ _e164(ZodE164, params));
	},
	datetime(params) {
		return this.check(/* @__PURE__ */ _isoDateTime(ZodISODateTime, params));
	},
	date(params) {
		return this.check(/* @__PURE__ */ _isoDate(ZodISODate, params));
	},
	time(params) {
		return this.check(/* @__PURE__ */ _isoTime(ZodISOTime, params));
	},
	duration(params) {
		return this.check(/* @__PURE__ */ _isoDuration(ZodISODuration, params));
	}
});
function string(params) {
	return /* @__PURE__ */ _string(ZodString, params);
}
const ZodStringFormat = /*@__PURE__*/ $constructor("ZodStringFormat", (inst, def) => {
	$ZodStringFormat.init(inst, def);
	_ZodString.init(inst, def);
});
const ZodISODateTime = /*@__PURE__*/ $constructor("ZodISODateTime", (inst, def) => {
	$ZodISODateTime.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodISODate = /*@__PURE__*/ $constructor("ZodISODate", (inst, def) => {
	$ZodISODate.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodISOTime = /*@__PURE__*/ $constructor("ZodISOTime", (inst, def) => {
	$ZodISOTime.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodISODuration = /*@__PURE__*/ $constructor("ZodISODuration", (inst, def) => {
	$ZodISODuration.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodEmail = /*@__PURE__*/ $constructor("ZodEmail", (inst, def) => {
	$ZodEmail.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodGUID = /*@__PURE__*/ $constructor("ZodGUID", (inst, def) => {
	$ZodGUID.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodUUID = /*@__PURE__*/ $constructor("ZodUUID", (inst, def) => {
	$ZodUUID.init(inst, def);
	ZodStringFormat.init(inst, def);
});
function uuid(params) {
	return /* @__PURE__ */ _uuid(ZodUUID, params);
}
const ZodURL = /*@__PURE__*/ $constructor("ZodURL", (inst, def) => {
	$ZodURL.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodEmoji = /*@__PURE__*/ $constructor("ZodEmoji", (inst, def) => {
	$ZodEmoji.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodNanoID = /*@__PURE__*/ $constructor("ZodNanoID", (inst, def) => {
	$ZodNanoID.init(inst, def);
	ZodStringFormat.init(inst, def);
});
/**
* @deprecated CUID v1 is deprecated by its authors due to information leakage
* (timestamps embedded in the id). Use {@link ZodCUID2} instead.
* See https://github.com/paralleldrive/cuid.
*/
const ZodCUID = /*@__PURE__*/ $constructor("ZodCUID", (inst, def) => {
	$ZodCUID.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodCUID2 = /*@__PURE__*/ $constructor("ZodCUID2", (inst, def) => {
	$ZodCUID2.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodULID = /*@__PURE__*/ $constructor("ZodULID", (inst, def) => {
	$ZodULID.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodXID = /*@__PURE__*/ $constructor("ZodXID", (inst, def) => {
	$ZodXID.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodKSUID = /*@__PURE__*/ $constructor("ZodKSUID", (inst, def) => {
	$ZodKSUID.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodIPv4 = /*@__PURE__*/ $constructor("ZodIPv4", (inst, def) => {
	$ZodIPv4.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodIPv6 = /*@__PURE__*/ $constructor("ZodIPv6", (inst, def) => {
	$ZodIPv6.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodCIDRv4 = /*@__PURE__*/ $constructor("ZodCIDRv4", (inst, def) => {
	$ZodCIDRv4.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodCIDRv6 = /*@__PURE__*/ $constructor("ZodCIDRv6", (inst, def) => {
	$ZodCIDRv6.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodBase64 = /*@__PURE__*/ $constructor("ZodBase64", (inst, def) => {
	$ZodBase64.init(inst, def);
	ZodStringFormat.init(inst, def);
});
function base64(params) {
	return /* @__PURE__ */ _base64(ZodBase64, params);
}
const ZodBase64URL = /*@__PURE__*/ $constructor("ZodBase64URL", (inst, def) => {
	$ZodBase64URL.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodE164 = /*@__PURE__*/ $constructor("ZodE164", (inst, def) => {
	$ZodE164.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodJWT = /*@__PURE__*/ $constructor("ZodJWT", (inst, def) => {
	$ZodJWT.init(inst, def);
	ZodStringFormat.init(inst, def);
});
const ZodNumber = /*@__PURE__*/ $constructor("ZodNumber", (inst, def) => {
	$ZodNumber.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => numberProcessor(inst, ctx, json, params);
	inst.isFinite = true;
}, /*@__PURE__*/ derived({
	minValue: (inst) => {
		const { minimum, exclusiveMinimum } = aggregateChecks(inst);
		return Math.max(minimum ?? Number.NEGATIVE_INFINITY, exclusiveMinimum ?? Number.NEGATIVE_INFINITY);
	},
	maxValue: (inst) => {
		const { maximum, exclusiveMaximum } = aggregateChecks(inst);
		return Math.min(maximum ?? Number.POSITIVE_INFINITY, exclusiveMaximum ?? Number.POSITIVE_INFINITY);
	},
	isInt: (inst) => {
		const { isInt, multipleOf } = aggregateChecks(inst);
		return !!isInt || !!multipleOf?.some(Number.isSafeInteger);
	},
	format: (inst) => aggregateChecks(inst).format ?? null
}, {
	gt(value, params) {
		return this.check(/* @__PURE__ */ _gt(value, params));
	},
	gte(value, params) {
		return this.check(/* @__PURE__ */ _gte(value, params));
	},
	min(value, params) {
		return this.check(/* @__PURE__ */ _gte(value, params));
	},
	lt(value, params) {
		return this.check(/* @__PURE__ */ _lt(value, params));
	},
	lte(value, params) {
		return this.check(/* @__PURE__ */ _lte(value, params));
	},
	max(value, params) {
		return this.check(/* @__PURE__ */ _lte(value, params));
	},
	int(params) {
		return this.check(int(params));
	},
	safe(params) {
		return this.check(int(params));
	},
	positive(params) {
		return this.check(/* @__PURE__ */ _gt(0, params));
	},
	nonnegative(params) {
		return this.check(/* @__PURE__ */ _gte(0, params));
	},
	negative(params) {
		return this.check(/* @__PURE__ */ _lt(0, params));
	},
	nonpositive(params) {
		return this.check(/* @__PURE__ */ _lte(0, params));
	},
	multipleOf(value, params) {
		return this.check(/* @__PURE__ */ _multipleOf(value, params));
	},
	step(value, params) {
		return this.check(/* @__PURE__ */ _multipleOf(value, params));
	},
	finite() {
		return this;
	}
}));
function number(params) {
	return /* @__PURE__ */ _number(ZodNumber, params);
}
const ZodNumberFormat = /*@__PURE__*/ $constructor("ZodNumberFormat", (inst, def) => {
	$ZodNumberFormat.init(inst, def);
	ZodNumber.init(inst, def);
});
function int(params) {
	return /* @__PURE__ */ _int(ZodNumberFormat, params);
}
const ZodBoolean = /*@__PURE__*/ $constructor("ZodBoolean", (inst, def) => {
	$ZodBoolean.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => booleanProcessor(inst, ctx, json, params);
});
function boolean(params) {
	return /* @__PURE__ */ _boolean(ZodBoolean, params);
}
const ZodNull = /*@__PURE__*/ $constructor("ZodNull", (inst, def) => {
	$ZodNull.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => nullProcessor(inst, ctx, json, params);
});
function _null(params) {
	return /* @__PURE__ */ _null$1(ZodNull, params);
}
const ZodUnknown = /*@__PURE__*/ $constructor("ZodUnknown", (inst, def) => {
	$ZodUnknown.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => void 0;
});
function unknown() {
	return /* @__PURE__ */ _unknown(ZodUnknown);
}
const ZodNever = /*@__PURE__*/ $constructor("ZodNever", (inst, def) => {
	$ZodNever.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => neverProcessor(inst, ctx, json, params);
});
function never(params) {
	return /* @__PURE__ */ _never(ZodNever, params);
}
const ZodArray = /*@__PURE__*/ $constructor("ZodArray", (inst, def) => {
	_ensureDefaultMemoizer();
	$ZodArray.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => arrayProcessor(inst, ctx, json, params);
	inst.element = def.element;
}, {
	min(n, params) {
		return this.check(/* @__PURE__ */ _minLength(n, params));
	},
	nonempty(params) {
		return this.check(/* @__PURE__ */ _minLength(1, params));
	},
	max(n, params) {
		return this.check(/* @__PURE__ */ _maxLength(n, params));
	},
	length(n, params) {
		return this.check(/* @__PURE__ */ _length(n, params));
	},
	unwrap() {
		return this.element;
	}
});
function array(element, params) {
	return /* @__PURE__ */ _array(ZodArray, element, params);
}
const ZodObject = /*@__PURE__*/ $constructor("ZodObject", (inst, def) => {
	_ensureDefaultMemoizer();
	$ZodObjectJIT.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => objectProcessor(inst, ctx, json, params);
	installLazyProp(inst, "shape", (self) => self._zod.def.shape, false);
}, {
	keyof() {
		return _enum(Object.keys(this._zod.def.shape));
	},
	catchall(catchall) {
		return this.clone(mergeDefs(this._zod.def, { catchall }));
	},
	passthrough() {
		return this.clone(mergeDefs(this._zod.def, { catchall: unknown() }));
	},
	loose() {
		return this.clone(mergeDefs(this._zod.def, { catchall: unknown() }));
	},
	strict() {
		return this.clone(mergeDefs(this._zod.def, { catchall: never() }));
	},
	strip() {
		return this.clone(mergeDefs(this._zod.def, { catchall: void 0 }));
	},
	extend(incoming) {
		return extend(this, incoming);
	},
	safeExtend(incoming) {
		return safeExtend(this, incoming);
	},
	merge(other) {
		return merge(this, other);
	},
	pick(mask) {
		return pick(this, mask);
	},
	omit(mask) {
		return omit(this, mask);
	},
	partial(...args) {
		return partial(ZodOptional, this, args[0]);
	},
	exactPartial(...args) {
		return partial(ZodExactOptional, this, args[0], "exactPartial");
	},
	required(...args) {
		return required(ZodNonOptional, this, args[0]);
	}
});
function object$1(shape, params) {
	const def = {
		type: "object",
		shape: shape ?? {},
		...normalizeParams(params)
	};
	return new ZodObject(def);
}
const ZodUnion = /*@__PURE__*/ $constructor("ZodUnion", (inst, def) => {
	$ZodUnion.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => unionProcessor(inst, ctx, json, params);
	inst.options = def.options;
});
function union(options, params) {
	return new ZodUnion({
		type: "union",
		options,
		...normalizeParams(params)
	});
}
const ZodDiscriminatedUnion = /*@__PURE__*/ $constructor("ZodDiscriminatedUnion", (inst, def) => {
	ZodUnion.init(inst, def);
	$ZodDiscriminatedUnion.init(inst, def);
});
function discriminatedUnion(discriminator, options, params) {
	return new ZodDiscriminatedUnion({
		type: "union",
		options,
		discriminator,
		...normalizeParams(params)
	});
}
const ZodIntersection = /*@__PURE__*/ $constructor("ZodIntersection", (inst, def) => {
	$ZodIntersection.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => intersectionProcessor(inst, ctx, json, params);
});
function intersection(left, right) {
	return new ZodIntersection({
		type: "intersection",
		left,
		right
	});
}
const ZodRecord = /*@__PURE__*/ $constructor("ZodRecord", (inst, def) => {
	_ensureDefaultMemoizer();
	$ZodRecord.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => recordProcessor(inst, ctx, json, params);
	inst.keyType = def.keyType;
	inst.valueType = def.valueType;
});
function record(keyType, valueType, params) {
	if (!valueType || !valueType._zod) return new ZodRecord({
		type: "record",
		keyType: string(),
		valueType: keyType,
		...normalizeParams(valueType)
	});
	return new ZodRecord({
		type: "record",
		keyType,
		valueType,
		...normalizeParams(params)
	});
}
function partialRecord(keyType, valueType, params) {
	return new ZodRecord({
		type: "record",
		keyType,
		valueType,
		...normalizeParams(params),
		partial: true
	});
}
const ZodEnum = /*@__PURE__*/ $constructor("ZodEnum", (inst, def) => {
	$ZodEnum.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => enumProcessor(inst, ctx, json, params);
	inst.enum = def.entries;
	inst.options = [...inst._zod.values];
	const keys = new Set(Object.keys(def.entries));
	inst.extract = (values, params) => {
		const newEntries = {};
		for (const value of values) if (keys.has(value)) newEntries[value] = def.entries[value];
		else throw new Error(`Key ${value} not found in enum`);
		return new ZodEnum({
			...def,
			checks: [],
			...normalizeParams(params),
			entries: newEntries
		});
	};
	inst.exclude = (values, params) => {
		const newEntries = { ...def.entries };
		for (const value of values) if (keys.has(value)) delete newEntries[value];
		else throw new Error(`Key ${value} not found in enum`);
		return new ZodEnum({
			...def,
			checks: [],
			...normalizeParams(params),
			entries: newEntries
		});
	};
});
function _enum(values, params) {
	const entries = Array.isArray(values) ? Object.fromEntries(values.map((v) => [v, v])) : values;
	return new ZodEnum({
		type: "enum",
		entries,
		...normalizeParams(params)
	});
}
const ZodLiteral = /*@__PURE__*/ $constructor("ZodLiteral", (inst, def) => {
	$ZodLiteral.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => literalProcessor(inst, ctx, json, params);
	inst.values = new Set(def.values);
	Object.defineProperty(inst, "value", { get() {
		if (def.values.length > 1) throw new Error("This schema contains multiple valid literal values. Use `.values` instead.");
		return def.values[0];
	} });
});
function literal(value, params) {
	return new ZodLiteral({
		type: "literal",
		values: Array.isArray(value) ? value : [value],
		...normalizeParams(params)
	});
}
const ZodTransform = /*@__PURE__*/ $constructor("ZodTransform", (inst, def) => {
	_ensureDefaultMemoizer();
	$ZodTransform.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => transformProcessor(inst, ctx, json, params);
	inst._zod.parse = (payload, _ctx) => {
		if (_ctx.direction === "backward") throw new $ZodEncodeError(inst.constructor.name);
		payload.addIssue = (issue$1) => {
			if (typeof issue$1 === "string") payload.issues.push(issue(issue$1, payload.value, def));
			else {
				const _issue = issue$1;
				if (_issue.fatal) _issue.continue = false;
				_issue.code ?? (_issue.code = "custom");
				if (!("input" in _issue)) _issue.input = payload.value;
				_issue.inst ?? (_issue.inst = inst);
				payload.issues.push(issue(_issue));
			}
		};
		const output = def.transform(payload.value, payload);
		if (output instanceof Promise) return output.then((output) => {
			payload.value = output;
			return payload;
		});
		payload.value = output;
		return payload;
	};
});
function transform(fn) {
	return new ZodTransform({
		type: "transform",
		transform: fn
	});
}
const ZodOptional = /*@__PURE__*/ $constructor("ZodOptional", (inst, def) => {
	$ZodOptional.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => optionalProcessor(inst, ctx, json, params);
	inst.unwrap = () => inst._zod.def.innerType;
});
function optional(innerType) {
	return new ZodOptional({
		type: "optional",
		innerType
	});
}
const ZodExactOptional = /*@__PURE__*/ $constructor("ZodExactOptional", (inst, def) => {
	$ZodExactOptional.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => optionalProcessor(inst, ctx, json, params);
	inst.unwrap = () => inst._zod.def.innerType;
});
function exactOptional(innerType) {
	return new ZodExactOptional({
		type: "optional",
		innerType
	});
}
const ZodNullable = /*@__PURE__*/ $constructor("ZodNullable", (inst, def) => {
	$ZodNullable.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => nullableProcessor(inst, ctx, json, params);
	inst.unwrap = () => inst._zod.def.innerType;
});
function nullable(innerType) {
	return new ZodNullable({
		type: "nullable",
		innerType
	});
}
const ZodDefault = /*@__PURE__*/ $constructor("ZodDefault", (inst, def) => {
	$ZodDefault.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => defaultProcessor(inst, ctx, json, params);
	inst.unwrap = () => inst._zod.def.innerType;
	inst.removeDefault = inst.unwrap;
});
function _default(innerType, defaultValue) {
	return new ZodDefault({
		type: "default",
		innerType,
		get defaultValue() {
			return typeof defaultValue === "function" ? defaultValue() : shallowClone(defaultValue);
		}
	});
}
const ZodPrefault = /*@__PURE__*/ $constructor("ZodPrefault", (inst, def) => {
	$ZodPrefault.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => prefaultProcessor(inst, ctx, json, params);
	inst.unwrap = () => inst._zod.def.innerType;
});
function prefault(innerType, defaultValue) {
	return new ZodPrefault({
		type: "prefault",
		innerType,
		get defaultValue() {
			return typeof defaultValue === "function" ? defaultValue() : shallowClone(defaultValue);
		}
	});
}
const ZodNonOptional = /*@__PURE__*/ $constructor("ZodNonOptional", (inst, def) => {
	$ZodNonOptional.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => nonoptionalProcessor(inst, ctx, json, params);
	inst.unwrap = () => inst._zod.def.innerType;
});
function nonoptional(innerType, params) {
	return new ZodNonOptional({
		type: "nonoptional",
		innerType,
		...normalizeParams(params)
	});
}
const ZodCatch = /*@__PURE__*/ $constructor("ZodCatch", (inst, def) => {
	$ZodCatch.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => catchProcessor(inst, ctx, json, params);
	inst.unwrap = () => inst._zod.def.innerType;
	inst.removeCatch = inst.unwrap;
});
function _catch(innerType, catchValue) {
	return new ZodCatch({
		type: "catch",
		innerType,
		catchValue: typeof catchValue === "function" ? catchValue : constantCatch(catchValue)
	});
}
const ZodPipe = /*@__PURE__*/ $constructor("ZodPipe", (inst, def) => {
	$ZodPipe.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => pipeProcessor(inst, ctx, json, params);
	inst.in = def.in;
	inst.out = def.out;
});
function pipe(in_, out) {
	return new ZodPipe({
		type: "pipe",
		in: in_,
		out
	});
}
const ZodReadonly = /*@__PURE__*/ $constructor("ZodReadonly", (inst, def) => {
	$ZodReadonly.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => readonlyProcessor(inst, ctx, json, params);
	inst.unwrap = () => inst._zod.def.innerType;
});
function readonly(innerType) {
	return new ZodReadonly({
		type: "readonly",
		innerType
	});
}
const ZodCustom = /*@__PURE__*/ $constructor("ZodCustom", (inst, def) => {
	$ZodCustom.init(inst, def);
	ZodType.init(inst, def);
	inst._zod.processJSONSchema = (ctx, json, params) => customProcessor(inst, ctx, json, params);
});
function refine(fn, _params = {}) {
	return /* @__PURE__ */ _refine(ZodCustom, fn, _params);
}
function superRefine(fn, params) {
	return /* @__PURE__ */ _superRefine(fn, params);
}
//#endregion
//#region node_modules/.pnpm/@deepseek-ai+dsh-ssh@0.2.2-rc.3_f1a732dbbc6593ea81eaaf0a0744d804/node_modules/@deepseek-ai/dsh-ssh/lib/protocol.js
const managementLimits = {
	heartbeat: 1,
	close: 1,
	"process.terminate": 128,
	"fs.streamClose": 128
};
function requestClass(method) {
	switch (method) {
		case "heartbeat":
		case "close":
		case "process.terminate":
		case "fs.streamClose": return method;
		default: return "ordinary";
	}
}
const errorSchema = object$1({
	name: string(),
	message: string(),
	code: string().optional()
}).strict();
const requestIdSchema = string().transform((value) => value);
const frameSchema = discriminatedUnion("type", [
	object$1({
		type: literal("request"),
		id: requestIdSchema,
		method: string(),
		params: unknown()
	}).strict(),
	object$1({
		type: literal("result"),
		id: requestIdSchema,
		value: unknown()
	}).strict(),
	object$1({
		type: literal("error"),
		id: requestIdSchema,
		error: errorSchema
	}).strict(),
	object$1({
		type: literal("cancel"),
		id: requestIdSchema
	}).strict()
]);
function operationError(error) {
	return error instanceof Error ? error : new Error(String(error));
}
/** A remote error retains its typed filesystem or sandbox code. */
var RemoteOperationError = class extends Error {
	code;
	constructor(message, code) {
		super(message);
		this.code = code;
		this.name = "RemoteOperationError";
	}
};
/** The peer owns pending calls and rejects ambiguous operations on connection loss; it never replays requests. */
var SshRpcPeer = class extends EventEmitter {
	input;
	output;
	maxFrameBytes;
	maxPending;
	handler;
	pending = /* @__PURE__ */ new Map();
	active = /* @__PURE__ */ new Map();
	writeTail = Promise.resolve();
	queuedBytes = 0;
	failure;
	constructor(input, output, maxFrameBytes, maxPending, handler) {
		super();
		this.input = input;
		this.output = output;
		this.maxFrameBytes = maxFrameBytes;
		this.maxPending = maxPending;
		this.handler = handler;
		input.on("error", (error) => {
			this.close(error);
		});
		output.on("error", (error) => {
			this.close(error);
		});
		output.on("close", () => {
			this.close();
		});
		this.readFrames().catch((error) => {
			this.close(operationError(error));
		});
	}
	/**
	* Send one request and validate its response before exposing it to the caller.
	* @param method - the private helper operation.
	* @param params - JSON request fields.
	* @param schema - validation for the remote response.
	* @param signal - cancellation without rollback of remote effects.
	* @returns the validated response or a transport/remote-operation rejection.
	*/
	async request(method, params, schema, signal) {
		signal?.throwIfAborted();
		if (this.failure !== void 0) throw this.failure;
		const kind = requestClass(method);
		if (this.atCapacity(kind, this.pending.values())) throw new Error("SSH helper pending request limit reached");
		const id = randomUUID();
		const result = Promise.withResolvers();
		result.promise.catch(() => {});
		this.pending.set(id, {
			...result,
			requestClass: kind
		});
		const abort = () => {
			result.reject(/* @__PURE__ */ new Error("SSH operation cancelled; a completed remote mutation is not rolled back"));
			this.send({
				type: "cancel",
				id
			}).catch(() => {});
		};
		signal?.addEventListener("abort", abort, { once: true });
		try {
			this.send({
				type: "request",
				id,
				method,
				params
			}).catch((error) => {
				this.pending.delete(id);
				result.reject(operationError(error));
			});
			return schema.parse(await result.promise);
		} finally {
			signal?.removeEventListener("abort", abort);
		}
	}
	/**
	* Fail pending operations and abort remote handlers without claiming rollback.
	* @param error - the transport failure reported to all pending operations.
	*/
	close(error = /* @__PURE__ */ new Error("SSH connection lost; remote operation outcome and cleanup are unknown")) {
		if (this.failure !== void 0) return;
		this.failure = error;
		for (const pending of this.pending.values()) pending.reject(error);
		this.pending.clear();
		for (const { controller } of this.active.values()) controller.abort(error);
		this.active.clear();
		this.input.destroy();
		this.output.destroy();
		this.emit("closed", error);
	}
	async send(frame) {
		if (this.failure !== void 0) return Promise.reject(this.failure);
		const body = Buffer.from(JSON.stringify(frame));
		if (body.length > this.maxFrameBytes || this.queuedBytes + body.length + 4 > this.maxFrameBytes * 2) return Promise.reject(/* @__PURE__ */ new Error("SSH helper frame or write queue limit exceeded"));
		const header = Buffer.alloc(4);
		header.writeUInt32BE(body.length);
		const bytes = Buffer.concat([header, body]);
		this.queuedBytes += bytes.length;
		const write = this.writeTail.then(async () => {
			if (this.failure !== void 0) throw this.failure;
			if (!this.output.write(bytes)) await new Promise((resolve, reject) => {
				const cleanup = () => {
					this.output.off("drain", drained);
					this.off("closed", closed);
				};
				const drained = () => {
					cleanup();
					resolve();
				};
				const closed = (error) => {
					cleanup();
					reject(error);
				};
				this.output.once("drain", drained);
				this.once("closed", closed);
				if (this.failure !== void 0) closed(this.failure);
			});
		});
		this.writeTail = write.catch((error) => {
			this.close(operationError(error));
		});
		return write.finally(() => {
			this.queuedBytes -= bytes.length;
		});
	}
	async readFrames() {
		const header = Buffer.alloc(4);
		let headerBytes = 0;
		let payload;
		let payloadBytes = 0;
		for await (const raw of this.input) {
			const chunk = Buffer.isBuffer(raw) ? raw : Buffer.from(raw);
			let offset = 0;
			while (offset < chunk.length) {
				if (payload === void 0) {
					const count = Math.min(4 - headerBytes, chunk.length - offset);
					chunk.copy(header, headerBytes, offset, offset + count);
					headerBytes += count;
					offset += count;
					if (headerBytes < 4) continue;
					const size = header.readUInt32BE(0);
					if (size === 0 || size > this.maxFrameBytes) throw new Error("SSH helper sent an invalid frame length");
					payload = Buffer.alloc(size);
					payloadBytes = 0;
				}
				const count = Math.min(payload.length - payloadBytes, chunk.length - offset);
				chunk.copy(payload, payloadBytes, offset, offset + count);
				payloadBytes += count;
				offset += count;
				if (payloadBytes === payload.length) {
					const frame = frameSchema.parse(JSON.parse(payload.toString("utf8")));
					payload = void 0;
					headerBytes = 0;
					this.receive(frame);
				}
			}
		}
		throw new Error(headerBytes > 0 || payload !== void 0 ? "SSH helper disconnected during a frame; outcome is unknown" : "SSH helper disconnected; outcome is unknown");
	}
	receive(frame) {
		if (frame.type === "result" || frame.type === "error") {
			const pending = this.pending.get(frame.id);
			if (pending === void 0) return;
			this.pending.delete(frame.id);
			if (frame.type === "result") pending.resolve(frame.value);
			else pending.reject(new RemoteOperationError(frame.error.message, frame.error.code));
			return;
		}
		if (frame.type === "cancel") {
			this.active.get(frame.id)?.controller.abort(/* @__PURE__ */ new Error("SSH caller cancelled the operation"));
			return;
		}
		const kind = requestClass(frame.method);
		if (this.handler === void 0 || this.active.has(frame.id) || this.atCapacity(kind, this.active.values())) throw new Error("SSH helper received an unexpected or excessive request");
		const controller = new AbortController();
		this.active.set(frame.id, {
			controller,
			requestClass: kind
		});
		this.handler(frame.method, frame.params, controller.signal).then((value) => this.send({
			type: "result",
			id: frame.id,
			value: value ?? null
		}), (error) => {
			const detail = operationError(error);
			const code = "code" in detail && typeof detail.code === "string" ? detail.code : void 0;
			return this.send({
				type: "error",
				id: frame.id,
				error: {
					name: detail.name,
					message: detail.message,
					...code === void 0 ? {} : { code }
				}
			});
		}).catch((error) => {
			this.close(operationError(error));
		}).finally(() => {
			this.active.delete(frame.id);
		});
	}
	atCapacity(kind, requests) {
		const limit = kind === "ordinary" ? this.maxPending : managementLimits[kind];
		let count = 0;
		for (const request of requests) if (request.requestClass === kind) count++;
		return count >= limit;
	}
};
//#endregion
//#region node_modules/.pnpm/@deepseek-ai+dsh-ssh@0.2.2-rc.3_f1a732dbbc6593ea81eaaf0a0744d804/node_modules/@deepseek-ai/dsh-ssh/lib/schemas.js
/** Strict JSON validation for SSH helper requests and remote observations. */
/** Admit a process identity from the private helper protocol. */
const processIdSchema = uuid().transform((value) => value);
/** Admit a text iterator identity from the private helper protocol. */
const textStreamIdSchema = uuid().transform((value) => value);
/** A remote POSIX absolute path; spelling is preserved until remote canonicalization. */
const remotePath = string().min(1).refine((value) => value.startsWith("/") && !value.includes("\0"), "expected an absolute POSIX path");
/** Filesystem identity returned by the remote filesystem provider. */
const targetSchema = object$1({
	targetKey: remotePath,
	displayPath: string()
}).strict();
object$1({
	version: string(),
	type: _enum([
		"file",
		"directory",
		"other"
	]),
	size: number().nonnegative().optional()
}).strict().extend({ type: _enum([
	"file",
	"directory",
	"symlink",
	"other"
]) });
/** A resolved file-effect policy; the remote helper owns path canonicalization. */
const policySchema = object$1({
	mode: _enum([
		"read-only",
		"workspace-write",
		"danger-full-access"
	]),
	workspaceRoot: remotePath,
	additionalWritableRoots: array(remotePath).optional(),
	sessionId: string().optional()
}).strict();
/** Complete directory entries. */
const entriesSchema = array(object$1({
	name: string(),
	type: _enum([
		"file",
		"directory",
		"other"
	]),
	target: targetSchema,
	version: string().optional(),
	size: number().nonnegative().optional()
}).strict());
/** Guarded write intent. */
const intentSchema = discriminatedUnion("kind", [object$1({ kind: literal("createIfAbsent") }).strict(), object$1({
	kind: literal("replaceIfVersion"),
	version: string()
}).strict()]);
/** Literal text edit. */
const editSchema = object$1({
	oldString: string(),
	newString: string(),
	replaceAll: boolean()
}).strict();
/** Atomic write observation. */
const writeResultSchema = object$1({
	operation: _enum(["create", "update"]),
	version: string(),
	before: string().nullable(),
	after: string()
}).strict();
/** Atomic edit observation. */
const editResultSchema = object$1({
	version: string(),
	before: string(),
	after: string()
}).strict();
/** Parsed patch hunk admitted at the remote JSON boundary. */
const hunkSchema = object$1({
	lineNumber: number().int().positive(),
	context: string().optional(),
	oldLines: array(string()),
	newLines: array(string()),
	isEndOfFile: boolean()
}).strict().transform(({ context, ...hunk }) => context === void 0 ? hunk : {
	...hunk,
	context
});
/** Source version guard used by patch, delete, and move. */
const versionGuardSchema = object$1({ version: string() }).strict();
/** Optional move patch and independent source/destination guards. */
const moveOptionsSchema = object$1({
	hunks: array(hunkSchema).optional(),
	expectedSource: versionGuardSchema.optional(),
	expectedDestination: intentSchema.optional()
}).strict();
/** Bounded, nullable content basis for a completed deletion. */
const deleteResultSchema = object$1({ before: string().nullable() }).strict();
/** Move completion preserves a published destination even when source removal fails. */
const moveResultSchema = discriminatedUnion("sourceDeleted", [object$1({
	sourceBefore: string().nullable(),
	destination: writeResultSchema,
	sourceDeleted: literal(true)
}).strict(), object$1({
	sourceBefore: string().nullable(),
	destination: writeResultSchema,
	sourceDeleted: literal(false),
	sourceFailure: object$1({
		code: _enum(FS_ERROR_CODES),
		message: string()
	}).strict()
}).strict()]);
/** Explicit child environment; null encodes an environment tombstone. */
const environmentSchema = record(string(), string().nullable());
const collection = object$1({
	maxBytes: number().int().positive(),
	spill: object$1({ maxBytes: number().int().positive() }).strict().optional()
}).strict();
/** Ordinary or terminal process requested by a trusted SSH client. */
const spawnSchema = object$1({
	argv: array(string().refine((value) => !value.includes("\0"))).min(1),
	cwd: remotePath,
	env: environmentSchema.optional(),
	graceMs: number().int().positive().max(2147483647),
	stdio: object$1({
		stdin: union([
			literal("ignore"),
			literal("pipe"),
			object$1({ data: string() }).strict()
		]),
		stdout: union([
			literal("pipe"),
			literal("inherit"),
			collection
		]),
		stderr: union([
			literal("pipe"),
			literal("inherit"),
			collection
		]),
		control: literal("pipe").optional()
	}).strict().optional(),
	terminal: object$1({
		terminalType: string().min(1),
		rows: number().int().positive(),
		cols: number().int().positive(),
		shellActivity: boolean().optional()
	}).strict().optional()
}).strict().refine((value) => value.stdio === void 0 !== (value.terminal === void 0), "select ordinary or terminal execution");
/** Connection handshake binds sockets and workspace to one helper process. */
const helloSchema = object$1({
	protocol: literal(3),
	hash: string().regex(/^[0-9a-f]{64}$/),
	kind: _enum(["node-script", "executable"]),
	platform: _enum(["linux", "darwin"]),
	nodeVersion: string(),
	executable: remotePath,
	root: remotePath,
	workspace: remotePath,
	bootstrapHash: string().regex(/^[0-9a-f]{64}$/).optional()
}).strict();
/** A prepared process publishes its sockets before target code may execute. */
const streamEndpointSchema = object$1({
	path: remotePath,
	capability: string().regex(/^[0-9a-f]{64}$/)
}).strict();
object$1({
	id: processIdSchema,
	streams: partialRecord(_enum([
		"stdin",
		"stdout",
		"stderr",
		"control",
		"terminal"
	]), streamEndpointSchema)
}).strict();
/** Direct process exit facts. */
const outcomeSchema = object$1({
	exitCode: number().int().nullable(),
	signal: string().nullable()
}).strict();
/** A bounded raw tail positioned in whole-stream byte coordinates. */
const outputSnapshotSchema = object$1({
	tail: base64(),
	totalBytes: number().int().nonnegative()
}).strict();
/**
* Bound one encoded tail and its RPC envelope by the declared collection budget.
* @param maxBytes - the collector's retained raw-byte limit.
* @returns the private snapshot frame limit, capped by the helper protocol ceiling.
*/
function outputSnapshotFrameLimit(maxBytes) {
	return Math.min(67108864, maxBytes * 2 + 1024);
}
object$1({
	outcome: outcomeSchema,
	spills: object$1({
		stdout: remotePath.optional(),
		stderr: remotePath.optional()
	}).strict(),
	collected: object$1({
		stdout: outputSnapshotSchema.optional(),
		stderr: outputSnapshotSchema.optional()
	}).strict()
}).strict();
object$1({
	processGroupId: number().int().positive(),
	inputWaiting: boolean()
}).strict().nullable();
object$1({
	state: _enum([
		"idle",
		"busy",
		"unknown"
	]),
	revision: number().int().nonnegative().max(Number.MAX_SAFE_INTEGER)
}).strict();
/** How many transport bytes one logical stream may push before yielding to other streams. */
const STREAM_CHUNK_BYTES = DEFAULT_STREAM_CHUNK_BYTES;
/** One logical stream: writable half sends data, readable half receives it. */
var MuxChannel = class extends Duplex {
	mux;
	id;
	constructor(mux, id) {
		super({
			allowHalfOpen: true,
			autoDestroy: false,
			emitClose: true
		});
		this.mux = mux;
		this.id = id;
	}
	_read() {
		this.mux.pullCredit(this.id);
	}
	_write(chunk, _encoding, callback) {
		this.mux.writeChannel(this.id, Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk), callback);
	}
	_final(callback) {
		this.mux.endChannel(this.id);
		callback();
	}
	_destroy(error, callback) {
		this.mux.destroyChannel(this.id, error);
		callback(error);
	}
};
/**
* Adapt a readable/writable pair into a transport.
*
* Errors are contained here — a closing peer's EPIPE is a close, not a crash —
* and `destroy` stops reading and half-closes the writable side, so a shared
* stdout is never destroyed outright.
*/
function streamTransport(input, output) {
	input.on("error", () => {});
	output.on("error", () => {});
	let destroyed = false;
	return {
		write: (frame) => {
			if (destroyed) return;
			try {
				output.write(frame);
			} catch {
				destroyed = true;
			}
		},
		on: (event, listener) => {
			if (event === "data" || event === "close" || event === "end") input.on(event, listener);
			else output.on("error", listener);
		},
		destroy: () => {
			destroyed = true;
			if (!input.destroyed) input.destroy();
			if (!output.destroyed && !output.writableEnded) output.end();
		}
	};
}
/** Multiplexes logical streams and opaque RPC bytes over one transport. */
var FrameMux = class {
	options;
	decoder;
	channels = /* @__PURE__ */ new Map();
	pendingAccepts = /* @__PURE__ */ new Set();
	jsonChannel;
	jsonPump;
	settled;
	settle = () => {};
	nextId = 1;
	failure;
	closing = false;
	constructor(options) {
		this.options = options;
		if (options.role === "server" && options.accept === void 0) throw new Error("a server-role mux requires an accept hook");
		this.decoder = new FrameDecoder(options.maxFrameBytes ?? 67108864);
		this.settled = new Promise((resolve) => {
			this.settle = resolve;
		});
		this.jsonPump = new PassThrough({ highWaterMark: 65536 });
		this.jsonChannel = new PassThrough({ highWaterMark: 65536 });
		this.jsonPump.on("data", (chunk) => {
			this.sendJson(chunk);
		});
		this.jsonPump.on("error", (error) => {
			this.close(error);
		});
		this.jsonChannel.on("error", () => {});
		options.transport.on("data", (chunk) => {
			this.receive(chunk);
		});
		options.transport.on("error", (error) => {
			this.close(error);
		});
		options.transport.on("close", () => {
			this.close(this.failure ?? /* @__PURE__ */ new Error("remote workspace transport closed"));
		});
		options.transport.on("end", () => {
			this.close(/* @__PURE__ */ new Error("remote workspace transport ended"));
		});
	}
	/** RPC bytes arriving from the peer; hand this to `SshRpcPeer` as its input. */
	jsonInput() {
		return this.jsonChannel;
	}
	/** Writable that carries this side's RPC frames to the peer. */
	jsonOutput() {
		return this.jsonPump;
	}
	/** Resolves once the transport failed or closed; `undefined` after a clean close. */
	get closed() {
		return this.settled;
	}
	get openStreams() {
		return this.channels.size;
	}
	/**
	* Attach a stream and wait for the peer to accept it.
	* @param endpoint - peer-interpreted coordinates (the remote server maps the path to its stream).
	* @param signal - cancels the attempt; a late acceptance is destroyed.
	* @returns the attached duplex.
	*/
	async open(endpoint, signal) {
		if (this.failure !== void 0) throw this.failure;
		if (this.options.role !== "client") throw new Error("only a client-role mux opens streams");
		signal?.throwIfAborted();
		const id = this.allocateId();
		const channel = new MuxChannel(this, id);
		const state = this.createState(id, channel);
		const ok = Promise.withResolvers();
		this.accepts.set(id, ok);
		const abort = () => {
			this.accepts.delete(id);
			this.send(encodeAbortFrame(id, "stream allocation cancelled"));
			this.removeState(state, /* @__PURE__ */ new Error("stream allocation cancelled"));
			ok.reject(signal?.reason instanceof Error ? signal.reason : new Error(String(signal?.reason)));
		};
		signal?.addEventListener("abort", abort, { once: true });
		try {
			this.send(encodeOpenFrame(id, endpoint));
			await ok.promise;
			return channel;
		} finally {
			signal?.removeEventListener("abort", abort);
		}
	}
	/** Close the transport and fail every stream and pending open. */
	close(reason) {
		if (process.env.DSH_RW_DEBUG) console.error("[mux] CLOSE role=", this.options.role, "reason=", reason?.message);
		if (this.failure !== void 0) return;
		const error = reason ?? /* @__PURE__ */ new Error("remote workspace channel closed");
		this.failure = error;
		this.closing = true;
		for (const pending of this.accepts.values()) pending.reject(error);
		this.accepts.clear();
		for (const state of [...this.channels.values()]) this.removeState(state, error);
		this.jsonPump.destroy();
		this.jsonChannel.destroy();
		this.options.transport.destroy();
		this.settle(reason);
	}
	/** Await outstanding server-side accept hooks (tests and orderly shutdown). */
	async quiesce() {
		while (this.pendingAccepts.size > 0) await Promise.allSettled([...this.pendingAccepts]);
	}
	/** @internal */
	writeChannel(id, bytes, done) {
		const state = this.channels.get(id);
		if (state === void 0) {
			done(/* @__PURE__ */ new Error("stream is no longer open"));
			return;
		}
		state.waiting = {
			bytes,
			done
		};
		this.pump(state);
	}
	/** @internal */
	endChannel(id) {
		const state = this.channels.get(id);
		if (state === void 0 || state.sentEnd) return;
		state.sentEnd = true;
		this.send(encodeEndFrame(id, END_DIRECTION.inbound));
		this.reap(state);
	}
	/** @internal */
	destroyChannel(id, error) {
		const state = this.channels.get(id);
		if (state === void 0) return;
		if (!state.sentEnd) this.send(encodeAbortFrame(id, error?.message ?? "stream destroyed"));
		this.removeState(state, error ?? void 0);
	}
	/** @internal */
	pullCredit(id) {
		const state = this.channels.get(id);
		if (state === void 0) return;
		state.blocked = false;
		this.offerCredit(state);
	}
	accepts = /* @__PURE__ */ new Map();
	allocateId() {
		for (let attempt = 0; attempt <= MAX_STREAM_ID; attempt++) {
			const id = this.nextId;
			this.nextId = this.nextId >= 4294967295 ? 1 : this.nextId + 1;
			if (!this.channels.has(id) && !this.accepts.has(id)) return id;
		}
		throw new Error("no stream ids remain");
	}
	createState(id, channel) {
		const state = {
			id,
			channel,
			sendWindow: this.options.windowBytes ?? 1048576,
			waiting: void 0,
			pendingCredit: 0,
			blocked: false,
			sentEnd: false,
			receivedEnd: false,
			settled: false
		};
		channel.on("error", () => {});
		this.channels.set(id, state);
		return state;
	}
	removeState(state, error) {
		if (state.settled) return;
		state.settled = true;
		this.channels.delete(state.id);
		if (state.waiting !== void 0) {
			const { done } = state.waiting;
			state.waiting = void 0;
			done(error ?? /* @__PURE__ */ new Error("stream closed before the write completed"));
		}
		if (!state.channel.destroyed) state.channel.destroy(error);
	}
	/** Drop the channel once both halves are done. */
	reap(state) {
		if (!state.receivedEnd || !state.channel.writableEnded) return;
		if (!state.channel.destroyed) state.channel.destroy();
		this.channels.delete(state.id);
	}
	send(frame) {
		if (process.env.DSH_RW_DEBUG && frame.readUInt8(0) === 0) console.error("[mux] SEND json role=", this.options.role, "len=", frame.length);
		if (this.failure !== void 0) return;
		try {
			this.options.transport.write(frame);
		} catch (error) {
			this.close(error instanceof Error ? error : new Error(String(error)));
		}
	}
	sendJson(bytes) {
		for (let offset = 0; offset < bytes.length; offset += STREAM_CHUNK_BYTES) this.send(encodeJsonFrame(bytes.subarray(offset, Math.min(bytes.length, offset + STREAM_CHUNK_BYTES))));
	}
	pump(state) {
		const waiting = state.waiting;
		if (waiting === void 0 || state.settled) return;
		if (state.sendWindow <= 0) return;
		const size = Math.min(waiting.bytes.length, state.sendWindow, STREAM_CHUNK_BYTES);
		const chunk = waiting.bytes.subarray(0, size);
		const rest = waiting.bytes.subarray(size);
		state.sendWindow -= size;
		state.waiting = rest.length === 0 ? void 0 : {
			bytes: rest,
			done: waiting.done
		};
		this.send(encodeDataFrame(state.id, chunk));
		if (rest.length === 0) waiting.done();
	}
	offerCredit(state) {
		if (state.blocked || state.pendingCredit === 0) return;
		const credit = state.pendingCredit;
		state.pendingCredit = 0;
		this.send(encodeCreditFrame(state.id, credit));
	}
	receive(chunk) {
		let frames;
		try {
			frames = this.decoder.push(chunk);
		} catch (error) {
			this.close(error);
			return;
		}
		for (const frame of frames) {
			if (this.failure !== void 0) return;
			try {
				this.dispatch(frame);
			} catch (error) {
				this.close(error);
				return;
			}
		}
	}
	dispatch(frame) {
		switch (frame.type) {
			case 0:
				this.jsonChannel.write(frame.json);
				return;
			case 1:
				this.receiveOpen(frame.id, frame.endpoint);
				return;
			case 5: {
				const pending = this.accepts.get(frame.id);
				if (pending === void 0) return;
				this.accepts.delete(frame.id);
				pending.resolve();
				return;
			}
			case 6: {
				const state = this.channels.get(frame.id);
				if (state !== void 0) this.removeState(state, new Error(frame.message));
				const pending = this.accepts.get(frame.id);
				if (pending === void 0) return;
				this.accepts.delete(frame.id);
				pending.reject(new Error(frame.message === "" ? "the remote refused the stream" : frame.message));
				return;
			}
			case 2: {
				const state = this.requireState(frame.id, "data");
				const accepted = state.channel.push(frame.payload);
				state.pendingCredit += frame.payload.length;
				if (!accepted) state.blocked = true;
				this.offerCredit(state);
				return;
			}
			case 3: {
				const state = this.channels.get(frame.id);
				if (state === void 0) return;
				if (frame.direction === END_DIRECTION.inbound) {
					state.receivedEnd = true;
					if (!state.channel.readableEnded) state.channel.push(null);
					this.reap(state);
				} else {
					state.receivedEnd = true;
					if (!state.channel.destroyed) state.channel.destroy();
					this.channels.delete(state.id);
				}
				return;
			}
			case 4: {
				const state = this.channels.get(frame.id);
				if (state !== void 0) this.removeState(state, new Error(frame.message === "" ? "the remote aborted the stream" : frame.message));
				return;
			}
			case 7: {
				const state = this.channels.get(frame.id);
				if (state === void 0) return;
				state.sendWindow += frame.bytes;
				this.pump(state);
				return;
			}
			default: throw new FrameError("unknown frame type");
		}
	}
	requireState(id, what) {
		const state = this.channels.get(id);
		if (state === void 0) throw new FrameError(`received ${what} for unknown stream ${id}`);
		return state;
	}
	receiveOpen(id, endpoint) {
		if (this.options.role !== "server") throw new FrameError("a client-role mux must not receive stream-open");
		if (this.channels.has(id)) throw new FrameError(`stream ${id} is already attached`);
		const accept = this.options.accept;
		const task = Promise.resolve().then(async () => accept(endpoint, id)).then(async (binding) => {
			if (this.failure !== void 0) {
				if (binding !== void 0) destroyBinding(binding);
				return;
			}
			if (binding === void 0) {
				this.send(encodeOpenFailFrame(id, "the remote has no stream for that endpoint"));
				return;
			}
			const channel = new MuxChannel(this, id);
			this.createState(id, channel);
			channel.on("data", (chunk) => {
				if (!binding.inbound.write(chunk)) channel.pause();
			});
			binding.inbound.on("drain", () => {
				channel.resume();
			});
			binding.inbound.on("error", (error) => {
				channel.destroy(error);
			});
			channel.on("end", () => {
				binding.inbound.end();
			});
			binding.outbound.on("data", (chunk) => {
				if (!channel.write(chunk)) binding.outbound.pause();
			});
			channel.on("drain", () => {
				binding.outbound.resume();
			});
			binding.outbound.on("error", (error) => {
				channel.destroy(error);
			});
			binding.outbound.on("end", () => {
				channel.end();
			});
			channel.on("error", () => {
				destroyBinding(binding);
			});
			channel.on("close", () => {
				destroyBinding(binding);
			});
			this.send(encodeOpenOkFrame(id));
		}).catch((error) => {
			this.send(encodeOpenFailFrame(id, error instanceof Error ? error.message : String(error)));
		}).finally(() => {
			this.pendingAccepts.delete(task);
		});
		this.pendingAccepts.add(task);
	}
};
/** Release both halves of a served stream without throwing on an already-closed side. */
function destroyBinding(binding) {
	if (!binding.inbound.destroyed) binding.inbound.destroy();
	if (!binding.outbound.destroyed) binding.outbound.destroy();
}
//#endregion
//#region node_modules/.pnpm/@deepseek-ai+dsh-subprocess-local@0.2.2-rc.3_@deepseek-ai+cordis@4.1.0_@deepseek-ai+dsh_c840bf9616a36afed6af97c61b152f36/node_modules/@deepseek-ai/dsh-subprocess-local/lib/output.js
/** Bounded output tails and private spill files shared by process providers. */
let spillCounter = 0;
let defaultSpillDir;
/**
* The default spill location: a private (0700) per-process directory under
* the OS tmpdir, created lazily. Predictable world-readable paths would let
* other local users read command output or pre-create symlinks. The directory
* is created once per process and never recreated: when an external
* temporary-file cleaner removes it while empty, the next spill open fails and
* the collector degrades to its in-memory tail. At a JavaScript-observable
* process exit the directory is removed only when it holds no completed spill
* file (spill files are retained as full-output recovery artifacts until an
* external cleanup).
*/
function privateSpillDir() {
	defaultSpillDir ??= mkdtempSync(join(tmpdir(), "dsh-subprocess-"));
	return defaultSpillDir;
}
/* v8 ignore next 4 -- exit listeners run after the coverage dump; removal is verified by the CI /tmp residue measurement. */
process.once("exit", () => {
	if (defaultSpillDir === void 0) return;
	try {
		rmdirSync(defaultSpillDir);
	} catch {}
});
/**
* The stderr reporter used when no owner supplies one: a bare
* {@link prepareManagedProcessBinding} caller has no plugin logger, and the
* failure must still reach the process diagnostics.
* @param error - the spill failure.
* @param label - the failed stream label.
*/
function reportSpillFailureToStderr(error, label) {
	process.stderr.write(`dsh-subprocess-local: ${label} spill failed; only the in-memory tail is retained: ${String(error)}\n`);
}
/**
* Build the reporter an owner passes as {@link SpillOptions.onFailure}: one
* error-level log line naming the owner and stream, with the failure appended
* so its `code`, `syscall`, and `path` reach the log.
* @param logger - the owner's plugin logger.
* @param owner - the component named in the line.
* @returns the reporter.
*/
function logSpillFailure(logger, owner) {
	return (error, label) => {
		const removedDirectory = error.code === "ENOENT";
		logger.error(`${owner} could not write the complete ${label} stream to its spill file; the result keeps only the in-memory tail and reports no full-output path.` + (removedDirectory ? " The spill directory no longer exists; a temporary-file cleaner removing it while empty is the usual cause." : ""), error);
	};
}
/**
* Prepare fallible output storage before starting a managed native process.
* This is the explicit resolve step for spill inputs: the spill directory
* defaults to the private per-process directory, and the failure reporter
* defaults to a stderr line when the caller has no logger.
* @param internals - optional caller-owned spill directory and failure reporter.
* @returns binding inputs whose spill directory is ready for use.
*/
function prepareManagedProcessBinding(internals = {}) {
	return {
		spillDir: internals.spillDir ?? privateSpillDir(),
		onSpillFailure: internals.onSpillFailure ?? reportSpillFailureToStderr
	};
}
/**
* Collects one stream with a bounded in-memory tail. With spill options, on
* first overflow a spill file is created and every chunk (including those
* already collected) is appended there while the full stream remains within
* the cap; without them, only the in-memory tail is ever retained (the
* diagnostic-tail shape — a language server's stderr).
*
* Spilling is best-effort: a spill open or write failure discards the spill,
* reports once through {@link SpillOptions.onFailure}, and never interrupts
* in-memory collection, because `push()` runs inside the stream's `'data'`
* listener where a thrown error would become an uncaught exception.
*
* Tail-keep rationale (pi/OpenCode): errors and final results cluster at the
* end of command output; the spill file covers the head.
*/
var OutputCollector = class {
	maxBytes;
	label;
	spill;
	chunks = [];
	bytes = 0;
	dropped = false;
	spillFd;
	spillFile;
	spillDisabled;
	/** Total bytes ever pushed (not just retained). */
	total = 0;
	/**
	* @param maxBytes - in-memory tail cap in bytes.
	* @param label - stream label used in spill file names and failure reports.
	* @param spill - spill storage; omit for tail-only collection.
	*/
	constructor(maxBytes, label, spill) {
		this.maxBytes = maxBytes;
		this.label = label;
		this.spill = spill;
		this.spillDisabled = spill === void 0;
	}
	/**
	* Ingest one stream chunk, counting it toward the whole-stream total. On
	* first overflow of the in-memory cap a spill file is opened (when spilling
	* is enabled) and every chunk (already-collected ones included) is appended
	* there from then on; the in-memory tail then drops whole chunks from its
	* head (or the head of a single over-cap chunk) until it fits the cap again.
	* @param chunk - the raw bytes from one stream 'data' event.
	*/
	push(chunk) {
		this.total += chunk.length;
		const overflows = this.bytes + chunk.length > this.maxBytes;
		const spill = this.spill;
		if (spill !== void 0 && !this.spillDisabled && (overflows || this.spillFd !== void 0)) this.spillAll(spill, chunk);
		this.chunks.push(chunk);
		this.bytes += chunk.length;
		while (this.bytes > this.maxBytes) {
			const head = this.chunks[0];
			const excess = this.bytes - this.maxBytes;
			if (head.length <= excess) {
				this.chunks.shift();
				this.bytes -= head.length;
			} else {
				this.chunks[0] = head.subarray(excess);
				this.bytes -= excess;
			}
			this.dropped = true;
		}
	}
	/**
	* Open the spill file lazily and append `chunk` (and any prior chunks once).
	* Runs inside the stream's `'data'` listener, so every filesystem failure is
	* contained here: the spill is discarded, reported once, and collection
	* continues with the in-memory tail alone.
	*/
	spillAll(spill, chunk) {
		if (this.total > spill.maxBytes) {
			this.discardSpill();
			return;
		}
		try {
			if (this.spillFd === void 0) {
				const file = join(spill.dir, `dsh-subprocess-${process.pid}-${++spillCounter}-${randomBytes(6).toString("hex")}-${this.label}.log`);
				const fd = openSync(file, "wx", 384);
				this.spillFile = file;
				this.spillFd = fd;
				for (const prior of this.chunks) writeSync(fd, prior);
			}
			writeSync(this.spillFd, chunk);
		} catch (error) {
			this.discardSpill();
			try {
				spill.onFailure(error, this.label);
			} catch (reporterFailure) {
				process.stderr.write(`dsh-subprocess-local: spill failure reporter threw: ${String(reporterFailure)}\n`);
			}
		}
	}
	/** Stop spilling and remove the file once it can no longer hold the complete stream. */
	discardSpill() {
		const fd = this.spillFd;
		const file = this.spillFile;
		this.spillFd = void 0;
		this.spillFile = void 0;
		this.spillDisabled = true;
		if (fd !== void 0) try {
			closeSync(fd);
		} catch {
			this.spillFd = fd;
		}
		if (file !== void 0) try {
			unlinkSync(file);
		} catch {}
	}
	/**
	* Incremental read in whole-stream byte coordinates: returns everything
	* pushed since `fromByte`. When `fromByte` has already slid out of the
	* in-memory tail window, the read is `lossy` — it returns the whole
	* retained tail and the gap is only recoverable from the spill file.
	* @param fromByte - whole-stream offset to resume from (a prior read's `nextOffset`; 0 for the first read).
	* @returns the delta text, the offset for the next read, the `lossy` flag, and the spill path when one was created.
	*/
	readFrom(fromByte) {
		const windowStart = this.total - this.bytes;
		const buffer = Buffer.concat(this.chunks);
		const lossy = fromByte < windowStart;
		return {
			text: (lossy ? buffer : buffer.subarray(fromByte - windowStart)).toString("utf8"),
			nextOffset: this.total,
			lossy,
			...this.spillFile !== void 0 ? { spillPath: this.spillFile } : {}
		};
	}
	/**
	* Copy the retained raw tail with its position in the complete observed stream.
	* @returns independent tail bytes and the total byte count before truncation.
	*/
	snapshot() {
		return {
			bytes: Buffer.concat(this.chunks),
			totalBytes: this.total
		};
	}
	/**
	* Close the spill file once the stream has ended. A failed close (delayed
	* writeback fault) stops advertising the spill path — the file may be
	* missing its tail — while every in-memory read keeps working. Idempotent;
	* the spawn path seals both collectors at settlement so reads after exit
	* never point at a still-open file.
	*/
	seal() {
		if (this.spillFd === void 0) return;
		try {
			closeSync(this.spillFd);
		} catch {
			this.spillFile = void 0;
		}
		this.spillFd = void 0;
	}
	/**
	* Seal the spill file and return the final output.
	* @returns the final collected output: tail text, truncation flag, and the spill path when intact.
	*/
	finalize() {
		this.seal();
		return {
			text: Buffer.concat(this.chunks).toString("utf8"),
			truncated: this.dropped,
			...this.spillFile !== void 0 ? { spillPath: this.spillFile } : {}
		};
	}
};
//#endregion
//#region src/server-plugin/execution/processes.ts
/**
* Remote process ownership.
*
* This mirrors the official SSH helper's `RemoteProcesses` (MIT, DeepSeek
* Harness `packages/ssh/ssh/src/helper-processes.ts`) with one substitution:
* where the helper binds one TLS-PSK unix socket per channel and makes the
* client attach them through OpenSSH forwarding, this server publishes the same
* `{path, capability}` endpoint coordinates but serves each one from a logical
* stream inside the multiplexer. The observable protocol is unchanged: a
* prepared process publishes its channels before any target code runs, `start`
* spawns, `done` reports the retained completion, and output collection pushes
* `snapshot` frames on the stream's own RPC channel.
*/
/** Resolve when a provider stream is finished, however it ended. */
function sourceFinished(source) {
	return new Promise((resolve) => {
		const done = () => {
			resolve();
		};
		source.once("end", done);
		source.once("close", done);
		source.once("error", done);
	});
}
/** Coalesce live tail updates while capture continues independently of network readers. */
var SnapshotForwarder = class {
	collector;
	peer;
	dirty = false;
	stopped = false;
	running;
	constructor(inbound, outbound, collector, maxBytes) {
		this.collector = collector;
		this.peer = new SshRpcPeer(inbound, outbound, outputSnapshotFrameLimit(maxBytes), 1);
		this.peer.once("closed", () => {
			this.stopped = true;
		});
	}
	offer() {
		if (this.stopped) return;
		this.dirty = true;
		if (this.running !== void 0) return;
		this.running = this.flush();
	}
	async flush() {
		try {
			while (this.dirty && !this.stopped) {
				this.dirty = false;
				const snapshot = this.collector.snapshot();
				await this.peer.request("snapshot", {
					tail: snapshot.bytes.toString("base64"),
					totalBytes: snapshot.totalBytes
				}, _null());
			}
		} catch {
			this.stopped = true;
			this.peer.close();
		} finally {
			this.running = void 0;
		}
	}
	async finish() {
		this.offer();
		while (this.running !== void 0) await this.running;
		if (!this.stopped) this.peer.close();
	}
};
/** Retained completions whose ids may still be awaited by the client. */
const COMPLETION_RETAIN_FACTOR = 4;
/** Owns remote launch reservations through final process-range quiescence. */
var RemoteProcesses = class {
	ctx;
	root;
	factory;
	limit;
	preparationMs;
	records = /* @__PURE__ */ new Map();
	/** Settled promises preserve the original rejection for late done requests. */
	completed = /* @__PURE__ */ new Map();
	cleanups = /* @__PURE__ */ new Set();
	closing = false;
	reportSpillFailure;
	constructor(ctx, root, factory, limit, preparationMs) {
		this.ctx = ctx;
		this.root = root;
		this.factory = factory;
		this.limit = limit;
		this.preparationMs = preparationMs;
		this.reportSpillFailure = logSpillFailure(ctx.logger, "remote workspace server");
	}
	/**
	* Allocate channel endpoints; no target executes until {@link start}.
	* @param raw - untrusted process request received over the channel.
	* @returns the reservation id and its endpoint coordinates.
	*/
	async prepare(raw) {
		if (this.closing || this.records.size >= this.limit) throw new Error("remote process capacity unavailable");
		const request = spawnSchema.parse(raw);
		const id = randomUUID();
		const directory = join(this.root, id);
		const controller = new AbortController();
		const record = {
			request,
			streams: {},
			controller,
			expiry: setTimeout(() => {
				this.release(id).catch(() => {});
			}, this.preparationMs)
		};
		this.records.set(id, record);
		try {
			await mkdir(directory, { mode: 448 });
			controller.signal.throwIfAborted();
			const names = request.terminal === void 0 ? [
				"stdout",
				"stderr",
				...request.stdio?.stdin === "pipe" ? ["stdin"] : [],
				...request.stdio?.control === "pipe" ? ["control"] : []
			] : ["terminal"];
			for (const name of names) {
				record.streams[name] = this.factory.create(name, join(directory, name), randomCapability());
				controller.signal.throwIfAborted();
			}
			return {
				id,
				streams: Object.fromEntries(Object.entries(record.streams).map(([name, stream]) => [name, {
					path: stream.path,
					capability: stream.capability
				}]))
			};
		} catch (error) {
			await this.release(id);
			throw error;
		}
	}
	/**
	* Start a prepared reservation. Duplicate starts refuse.
	* @param id - the prepared reservation.
	* @param signal - cancels pending publication without claiming rollback.
	* @returns the terminal pid when the request owns a PTY.
	*/
	async start(id, signal) {
		const record = this.record(id);
		if (record.start !== void 0) throw new Error("remote process launch was already requested");
		signal?.throwIfAborted();
		const abort = () => {
			record.controller.abort(signal?.reason);
		};
		signal?.addEventListener("abort", abort, { once: true });
		record.start = this.startOnce(id, record);
		try {
			await record.start;
		} catch (error) {
			const failed = Promise.reject(error);
			failed.catch(() => {});
			record.done = failed;
			await this.finishFailed(id, record, failed);
			throw error;
		} finally {
			signal?.removeEventListener("abort", abort);
		}
		return record.terminal === void 0 ? {} : { pid: record.terminal.pid };
	}
	async startOnce(id, record) {
		clearTimeout(record.expiry);
		if (this.closing) throw new Error("remote workspace server is closing");
		record.controller.signal.throwIfAborted();
		const request = record.request;
		const resolved = await this.ctx.fs.resolve(request.cwd, { signal: record.controller.signal });
		const cwd = this.ctx.fs.processPath(resolved);
		record.controller.signal.throwIfAborted();
		const env = request.env === void 0 ? {} : Object.fromEntries(Object.entries(request.env).map(([key, value]) => [key, value ?? void 0]));
		if (request.terminal !== void 0) {
			const terminalEnv = Object.fromEntries(Object.entries(env).filter((entry) => entry[1] !== void 0));
			terminalEnv["PATH"] = mergedPath(env["PATH"], process.env["PATH"]) ?? "/usr/local/bin:/usr/bin:/bin";
			const diagnostic = {
				argv: request.argv,
				cwd,
				path: terminalEnv["PATH"],
				uid: process.getuid?.()
			};
			let terminal;
			try {
				terminal = await this.ctx.subprocess.spawnTerminal({
					argv: request.argv,
					cwd,
					env: terminalEnv,
					graceMs: request.graceMs,
					...request.terminal,
					signal: record.controller.signal
				});
			} catch (error) {
				throw diagnoseSpawn(error, diagnostic);
			}
			record.terminal = terminal;
			record.controller.signal.throwIfAborted();
			const stream = record.streams.terminal;
			stream.inbound.resume();
			const output = pipeline(terminal.output, stream.outbound).catch(() => {});
			const done = terminal.done.then((outcome) => ({
				outcome: wireOutcome(outcome),
				spills: {},
				collected: {}
			})).catch((error) => {
				throw diagnoseSpawn(error, diagnostic);
			});
			record.done = done;
			done.then(async () => {
				if (request.terminal?.shellActivity === true) {
					await output;
					return;
				}
				await terminal.terminate();
				await output;
				await this.rememberCompleted(id, record, done);
			}, () => this.finishFailed(id, record, done)).catch(() => {});
			return;
		}
		const stdio = request.stdio;
		const spawnEnv = { ...env };
		spawnEnv["PATH"] = mergedPath(env["PATH"], process.env["PATH"]) ?? "/usr/local/bin:/usr/bin:/bin";
		const diagnostic = {
			argv: request.argv,
			cwd,
			path: spawnEnv["PATH"],
			uid: process.getuid?.()
		};
		const spec = {
			argv: request.argv,
			cwd,
			env: spawnEnv,
			graceMs: request.graceMs,
			signal: record.controller.signal,
			stdio: {
				stdin: stdio.stdin,
				stdout: "pipe",
				stderr: "pipe",
				...stdio.control === void 0 ? {} : { control: stdio.control }
			}
		};
		let ordinary;
		try {
			ordinary = this.ctx.subprocess.spawn(spec);
		} catch (error) {
			throw diagnoseSpawn(error, diagnostic);
		}
		record.ordinary = ordinary;
		const control = ordinary.control;
		if (record.streams.control !== void 0 && control === void 0) {
			ordinary.done.catch(() => {});
			throw new Error("remote subprocess provider did not establish the control channel");
		}
		const collectors = {};
		const stopCapture = [];
		const forwarders = [];
		const forwarded = [];
		const streams = [];
		for (const name of ["stdout", "stderr"]) {
			const source = ordinary[name];
			const mode = stdio[name];
			const stream = record.streams[name];
			if (typeof mode === "object") {
				const binding = prepareManagedProcessBinding({ onSpillFailure: this.reportSpillFailure });
				const collector = new OutputCollector(mode.maxBytes, name, mode.spill === void 0 ? void 0 : {
					maxBytes: mode.spill.maxBytes,
					dir: binding.spillDir,
					onFailure: binding.onSpillFailure
				});
				collectors[name] = collector;
				const forwarder = new SnapshotForwarder(stream.inbound, stream.outbound, collector, mode.maxBytes);
				forwarders.push(forwarder);
				const receive = (chunk) => {
					collector.push(chunk);
					forwarder.offer();
				};
				source.on("data", receive);
				stopCapture.push(() => {
					source.off("data", receive);
					source.destroy();
					collector.seal();
				});
			} else {
				stream.inbound.resume();
				pipeline(source, stream.outbound).catch(() => {});
				streams.push(sourceFinished(source));
			}
		}
		if (record.streams.stdin !== void 0) {
			const stream = record.streams.stdin;
			pipeline(stream.inbound, ordinary.stdin).catch(() => {});
			stream.outbound.resume();
		}
		if (record.streams.control !== void 0) {
			const duplex = control;
			const stream = record.streams.control;
			stream.inbound.pipe(duplex);
			duplex.pipe(stream.outbound);
			stream.inbound.on("error", () => {
				duplex.destroy();
			});
			duplex.on("error", () => {
				stream.inbound.destroy();
			});
		}
		const done = ordinary.done.finally(() => {
			for (const stop of stopCapture) stop();
		}).catch((error) => {
			throw diagnoseSpawn(error, diagnostic);
		}).then(async (outcome) => {
			for (const forwarder of forwarders) forwarded.push(forwarder.finish());
			await Promise.race([Promise.all(streams), new Promise((resolve) => {
				setTimeout(resolve, request.graceMs).unref();
			})]);
			const spills = {};
			const collected = {};
			for (const name of ["stdout", "stderr"]) {
				const collector = collectors[name];
				if (collector === void 0) continue;
				const snapshot = collector.snapshot();
				collected[name] = {
					tail: snapshot.bytes.toString("base64"),
					totalBytes: snapshot.totalBytes
				};
				const path = collector.readFrom(0).spillPath;
				if (path !== void 0) spills[name] = path;
			}
			return {
				outcome: wireOutcome(outcome),
				spills,
				collected
			};
		});
		record.done = done;
		done.then(async () => {
			await ordinary.waitForExit();
			await Promise.all([...streams, ...forwarded]);
			await this.rememberCompleted(id, record, done);
		}, () => this.finishFailed(id, record, done)).catch(() => {});
	}
	/**
	* Await the direct result without claiming descendants have exited.
	* @param id - the started reservation.
	* @returns the exit observation and retained output.
	*/
	async done(id) {
		const retained = this.completed.get(id);
		if (retained !== void 0) return retained;
		const record = this.record(id);
		if (record.start === void 0) throw new Error("remote process has not started");
		await record.start;
		return record.done;
	}
	/**
	* Observe the managed range used for termination.
	* @param id - the started reservation.
	* @param signal - cancels the observation, leaving ownership intact.
	* @returns whether the owned range is empty.
	*/
	async wait(id, signal) {
		if (this.completed.has(id)) return true;
		const record = this.record(id);
		await record.start;
		if (record.ordinary !== void 0) return record.ordinary.waitForExit(signal);
		if (record.terminal !== void 0) {
			await record.terminal.terminate();
			return true;
		}
		throw new Error("remote process was not started");
	}
	/** Terminate and await the managed range independently of output readers. */
	async terminate(id) {
		if (this.completed.has(id)) return;
		const record = this.record(id);
		record.controller.abort(/* @__PURE__ */ new Error("remote process termination requested"));
		record.ordinary?.terminate();
		if (record.ordinary !== void 0) await record.ordinary.waitForExit();
		if (record.terminal !== void 0) {
			await record.terminal.terminate();
			if (record.request.terminal?.shellActivity === true && record.done !== void 0) await this.rememberCompleted(id, record, record.done);
		}
		if (record.ordinary === void 0 && record.terminal === void 0) await this.release(id);
	}
	/**
	* Operate on the terminal owned by one reservation.
	* @param id - the terminal reservation.
	* @param operation - input, foreground observation, or signal delivery.
	* @param value - input text or signal name.
	* @returns the operation's wire result.
	*/
	async terminal(id, operation, value) {
		const terminal = this.record(id).terminal;
		if (terminal === void 0) throw new Error("remote handle does not own a terminal");
		if (operation === "write") {
			await terminal.write(string().parse(value));
			return null;
		}
		if (operation === "inspect") return await terminal.inspectForeground() ?? null;
		if (operation === "activity") return await terminal.inspectActivity();
		return await terminal.signalForeground(terminalSignalSchema.parse(value));
	}
	/** Resize an allocated terminal without replacing its process. */
	async resizeTerminal(id, cols, rows) {
		const terminal = this.record(id).terminal;
		if (terminal === void 0) throw new Error("remote handle does not own a terminal");
		await terminal.resize(cols, rows);
	}
	/** Stop every owned process on lease expiry or disconnect. */
	async close() {
		if (this.closing) return;
		this.closing = true;
		const outcomes = await Promise.allSettled([...[...this.records.keys()].map((id) => this.release(id)), ...this.cleanups]);
		const errors = [...new Set(outcomes.flatMap((result) => result.status === "rejected" ? [result.reason] : []))];
		if (errors.length > 0) throw new AggregateError(errors, "remote process cleanup failed");
	}
	async release(id) {
		const record = this.records.get(id);
		if (record === void 0) return;
		record.release ??= this.trackCleanup(async () => {
			clearTimeout(record.expiry);
			record.controller.abort(/* @__PURE__ */ new Error("remote process reservation closed"));
			await record.start?.catch(() => {});
			for (const stream of Object.values(record.streams)) this.factory.close(stream);
			record.ordinary?.terminate();
			if (record.ordinary !== void 0) await record.ordinary.waitForExit();
			if (record.terminal !== void 0) await record.terminal.terminate();
			this.records.delete(id);
			await rm(join(this.root, id), {
				recursive: true,
				force: true
			});
		});
		await record.release;
	}
	record(id) {
		const record = this.records.get(id);
		if (record === void 0) throw new Error("unknown or expired remote process handle");
		return record;
	}
	async finishFailed(id, record, result) {
		clearTimeout(record.expiry);
		record.ordinary?.terminate();
		if (record.ordinary !== void 0) await record.ordinary.waitForExit();
		if (record.terminal !== void 0) await record.terminal.terminate();
		await this.rememberCompleted(id, record, result);
	}
	async rememberCompleted(id, record, result) {
		if (this.records.get(id) !== record || record.release !== void 0) return;
		const cleanup = this.trackCleanup(async () => {
			for (const stream of Object.values(record.streams)) this.factory.close(stream);
			await rm(join(this.root, id), {
				recursive: true,
				force: true
			});
		});
		this.records.delete(id);
		this.completed.set(id, result);
		if (this.completed.size > this.limit * COMPLETION_RETAIN_FACTOR) this.completed.delete(this.completed.keys().next().value);
		await cleanup;
	}
	trackCleanup(work) {
		const pending = Promise.resolve().then(work);
		this.cleanups.add(pending);
		return pending.finally(() => {
			this.cleanups.delete(pending);
		});
	}
};
/** Spawn failure enriched with the exact environment the provider saw (protocol §16.6). */
const SPAWN_DIAGNOSED = Symbol.for("dsh-rw.spawn-diagnosed");
/** Attach a scrubbed one-line diagnostic to a spawn failure without losing its code. */
function diagnoseSpawn(error, context) {
	const detail = error instanceof Error ? error : new Error(String(error));
	if (detail[SPAWN_DIAGNOSED] === true) return detail;
	const code = detail.code;
	const report = {
		name: detail.name,
		...typeof code === "string" ? { code } : {},
		message: detail.message,
		argv: [...context.argv],
		cwd: context.cwd,
		path: context.path,
		uid: context.uid
	};
	const safeReport = scrubSecrets(JSON.stringify(report));
	const wrapped = new Error(`${scrubSecrets(detail.message)} [spawn-diagnostic ${safeReport}]`, { cause: detail });
	Object.defineProperty(wrapped, SPAWN_DIAGNOSED, { value: true });
	if (typeof code === "string") Object.defineProperty(wrapped, "code", {
		value: code,
		enumerable: true,
		configurable: true
	});
	return wrapped;
}
/** Keep the profile's PATH usable when a client env omits or narrows the server dirs. */
function mergedPath(requested, serverPath) {
	const server = serverPath === void 0 || serverPath === "" ? void 0 : serverPath;
	const defaults = "/usr/local/bin:/usr/bin:/bin";
	if (requested === void 0 || requested === "") return [server, defaults].filter((entry) => entry !== void 0).join(":") || void 0;
	return [
		requested,
		server,
		defaults
	].filter((entry) => entry !== void 0 && entry !== "").join(":");
}
const terminalSignalSchema = _enum([
	"SIGINT",
	"SIGTERM",
	"SIGKILL",
	"SIGTSTP",
	"SIGHUP"
]);
/** Convert a provider outcome into its wire shape with an exception-free signal. */
function wireOutcome(outcome) {
	return {
		exitCode: outcome.exitCode,
		signal: outcome.signal === null ? null : String(outcome.signal)
	};
}
/** One 256-bit stream capability, hex encoded. */
function randomCapability() {
	return randomUUID().replaceAll("-", "") + randomUUID().replaceAll("-", "");
}
//#endregion
//#region src/server-plugin/execution/controller.ts
/**
* The remote execution-world server.
*
* It answers the official SSH helper's method set (see
* `packages/ssh/ssh/src/helper.ts`, MIT, DeepSeek Harness) over this plugin's
* multiplexed channel, translating every method into calls on the providers
* already mounted in the remote profile (`ctx.fs`, `ctx.subprocess`,
* `ctx.sandbox`) instead of building its own. That is the point of running a
* *profile* on the remote rather than uploading a helper: the execution world is
* the same official provider set, so the local client reuses the official SSH
* providers unchanged.
*/
/** Constant-time stream-capability comparison (the old PSK helper's admission rule). */
function capabilityMatches(expected, candidate) {
	if (typeof candidate !== "string" || candidate.length !== expected.length) return false;
	const left = Buffer.from(expected, "utf8");
	const right = Buffer.from(candidate, "utf8");
	return left.length === right.length && timingSafeEqual(left, right);
}
/** The official helper's whole-text and read ceilings, re-applied on this side. */
const MAX_TEXT_BYTES = 8388608;
/** Ordinary in-flight requests; management methods have reserved capacity in `SshRpcPeer`. */
const MAX_PENDING = 128;
/** How long a prepared reservation may wait for `process.start`. */
const PREPARATION_MS = 3e4;
const object = object$1({}).strict();
/** Admit a wire target as a filesystem identity; spelling is re-canonicalized remotely. */
const asTarget = (raw) => targetSchema.parse(raw);
/** The same admission as a request field, so every parsed target is already branded. */
const targetField = targetSchema.transform((value) => value);
/** Guarded-write intents carry the observed version the remote provider compares. */
const intentField = intentSchema.transform((value) => value);
/** The observed version an edit expects, branded for the provider. */
const versionField = object$1({ version: string() }).strict().transform((value) => value);
const processIdRequest = object$1({ id: processIdSchema }).strict();
const streamIdRequest = object$1({ id: textStreamIdSchema }).strict();
const pathRequest = object$1({
	path: string(),
	cwd: string().startsWith("/").optional()
}).strict();
const targetRequest = object$1({ target: targetField }).strict();
const writeRequest = object$1({
	target: targetField,
	content: string(),
	expected: intentField.optional(),
	policy: policySchema
}).strict();
const editRequest = object$1({
	target: targetField,
	edit: editSchema,
	expected: versionField.optional(),
	policy: policySchema
}).strict();
const patchRequest = object$1({
	target: targetField,
	hunks: array(hunkSchema),
	expected: versionField.optional(),
	policy: policySchema
}).strict();
const deleteRequest = object$1({
	target: targetField,
	expected: versionField.optional(),
	policy: policySchema
}).strict();
const moveRequest = object$1({
	source: targetField,
	destination: targetField,
	options: moveOptionsSchema.optional(),
	policy: policySchema
}).strict();
const readBytesRequest = object$1({
	target: targetField,
	maxBytes: number().int().positive()
}).strict();
const readRangeRequest = object$1({
	target: targetField,
	offset: number().int().nonnegative(),
	length: number().int().nonnegative()
}).strict();
const executableRequest = object$1({
	command: string(),
	env: environmentSchema.optional()
}).strict();
const sandboxRequest = object$1({
	argv: array(string()).min(1),
	policy: policySchema
}).strict();
/** Modes already reported as unconfined so the log stays one line per mode. */
const unconfinedReported = /* @__PURE__ */ new Set();
/**
* The reply for a confined-mode request this world cannot confine.
*
* The shape is a **wire contract**, not an implementation detail:
* `@deepseek-ai/dsh-sandbox-ssh` parses the `sandbox` reply with a strict facts
* schema (`{ argv, enforcement, denialSignatures, runnerFailureRules }` — see
* its `factsSchema`) and converts **any** parse failure into
* `SandboxUnavailableError`. Returning the bare argv array therefore produced
* "sandbox mode … is requested but no sandbox backend is usable on this host"
* (with Linux/macOS/Windows advice) for a command that actually ran on the
* remote host — alpha54, and the `Runner failure: [` tail was zod's JSON dump of
* the rejected array. An earlier round verified this call with `z.unknown()`,
* which is exactly why the mismatch survived: the client never parsed it.
*
* `enforcement: 'partial'` is the honest value — no backend governs these file
* effects (the service definition reserves `full` for an argv a backend actually
* wrapped). Empty signatures/rules say this world adds no denial or
* runner-failure detection of its own.
* @param argv - the caller's argv, executed as-is inside this remote world.
*/
function unconfinedSandboxFacts(argv) {
	return {
		argv: [...argv],
		enforcement: "partial",
		denialSignatures: [],
		runnerFailureRules: []
	};
}
/** Say once per mode that the remote world cannot confine and runs as-is. */
function noteUnconfined(mode) {
	if (unconfinedReported.has(mode)) return;
	unconfinedReported.add(mode);
	console.error(`[dsh-remote-workspace] no sandbox backend inside the remote world; running \`${mode}\` commands unconfined there`);
}
const resizeRequest = object$1({
	id: processIdSchema,
	cols: number().int().positive(),
	rows: number().int().positive()
}).strict();
const terminalOperation = object$1({
	id: processIdSchema,
	value: string().optional()
}).strict();
const helloRequest = object$1({
	protocol: literal(3),
	workspace: string().startsWith("/"),
	leaseMs: number().int().min(3e3).max(6e5),
	bootstrapPath: string().startsWith("/").optional()
}).strict();
const endpointSchema = object$1({
	path: string(),
	capability: string().regex(/^[0-9a-f]{64}$/)
}).strict();
/** Server half of the remote-workspace channel. */
var RemoteExecutionServer = class {
	options;
	mux;
	peer;
	streams = /* @__PURE__ */ new Map();
	iterators = /* @__PURE__ */ new Map();
	lifetime = new AbortController();
	processes;
	lease;
	/** Holds the host process open while this server is serving. */
	keepAlive;
	leaseMs;
	root = "";
	initialized = false;
	cleanup;
	workspace;
	hostDigest;
	constructor(options) {
		this.options = options;
		if (process.platform !== "linux" && process.platform !== "darwin") throw new Error("the remote workspace server requires a POSIX host");
		this.workspace = options.workspace;
		this.leaseMs = options.leaseMs ?? 3e4;
		this.keepAlive = setInterval(() => {}, 1 << 30);
		this.mux = new FrameMux({
			transport: streamTransport(options.input, options.output),
			role: "server",
			accept: (endpoint) => this.accept(endpoint),
			label: "remote-workspace-server"
		});
		this.peer = new SshRpcPeer(this.mux.jsonInput(), this.mux.jsonOutput(), DEFAULT_MAX_FRAME_BYTES, MAX_PENDING, (method, params, signal) => this.dispatch(method, params, signal));
	}
	/** Resolve once the server root exists; every method awaits it. */
	get ready() {
		return this.started;
	}
	started = (async () => {
		this.root = await mkdtemp(join(tmpdir(), "dsh-remote-"));
		this.processes = new RemoteProcesses(this.options.ctx, this.root, {
			create: (name, path, capability) => this.createStream(name, path, capability),
			close: (stream) => this.closeStream(stream)
		}, this.options.maxProcesses ?? 128, PREPARATION_MS);
	})();
	/**
	* Attach one published endpoint to its logical stream.
	* @param endpoint - untrusted `{path, capability}` from the client.
	* @returns the directional binding for that stream, or `undefined` for a refusal.
	*/
	accept(endpoint) {
		const parsed = endpointSchema.safeParse(endpoint);
		if (!parsed.success) return void 0;
		const stream = this.streams.get(parsed.data.path);
		if (stream === void 0) return void 0;
		if (!capabilityMatches(stream.capability, parsed.data.capability)) return void 0;
		this.streams.delete(parsed.data.path);
		return {
			inbound: stream.inbound,
			outbound: stream.outbound
		};
	}
	createStream(name, path, capability) {
		const stream = {
			name,
			path,
			capability,
			inbound: new PassThrough(),
			outbound: new PassThrough()
		};
		stream.inbound.on("error", () => {});
		stream.outbound.on("error", () => {});
		this.streams.set(path, stream);
		return stream;
	}
	closeStream(stream) {
		this.streams.delete(stream.path);
		if (!stream.inbound.destroyed) stream.inbound.destroy();
		if (!stream.outbound.destroyed) stream.outbound.destroy();
	}
	/** Release every stream, process and text iterator, then close the channel. */
	dispose() {
		this.cleanup ??= this.release().finally(() => {
			if (this.keepAlive !== void 0) clearInterval(this.keepAlive);
			this.peer.close();
			this.mux.close();
		});
		return this.cleanup;
	}
	/**
	* Release owned resources while keeping the channel able to answer.
	*
	* A `close` request must be *answered* — the peer that asked for it is still
	* listening — so the handler releases first and lets the transport close
	* afterwards, either through the client or through the dedicated host's exit.
	*/
	release() {
		return (async () => {
			if (this.lease !== void 0) clearTimeout(this.lease);
			this.lifetime.abort(/* @__PURE__ */ new Error("remote workspace server is closing"));
			for (const record of this.iterators.values()) record.controller.abort(this.lifetime.signal.reason);
			await Promise.allSettled([...this.iterators.values()].map(async (record) => record.iterator.return?.()));
			this.iterators.clear();
			for (const stream of [...this.streams.values()]) this.closeStream(stream);
			await this.started.catch(() => {});
			try {
				await this.processes?.close();
			} finally {
				await rm(this.root, {
					recursive: true,
					force: true
				});
			}
		})();
	}
	touchLease() {
		if (this.lease !== void 0) clearTimeout(this.lease);
		this.lease = setTimeout(() => {
			this.dispose().catch(() => {});
		}, this.leaseMs);
		this.lease.unref();
	}
	/**
	* Resolve every policy path on this host before the provider sees it.
	*
	* The client's `sandboxPolicy.resolve()` speaks about *its* world; the remote
	* provider fences against paths in *this* one, so the workspace root and each
	* additional writable root are canonicalized here. A root that cannot be
	* resolved fails the call: the enforcing backend rejects unusable roots per
	* call, and silently dropping one would widen or narrow the fence invisibly.
	*/
	async resolvePolicy(raw, signal) {
		const parsed = policySchema.parse(raw);
		const canonical = async (path) => {
			const target = await this.options.ctx.fs.resolve(path, { signal });
			return this.options.ctx.fs.processPath(target);
		};
		const roots = parsed.mode === "workspace-write" ? parsed.additionalWritableRoots : void 0;
		return {
			...parsed,
			workspaceRoot: await canonical(parsed.workspaceRoot),
			...roots === void 0 ? {} : { additionalWritableRoots: await Promise.all(roots.map(async (root) => await canonical(root))) }
		};
	}
	async dispatch(method, raw, requestSignal) {
		if (process.env.DSH_RW_DEBUG) console.error("[srv] dispatch", method);
		const { ctx } = this.options;
		const signal = AbortSignal.any([requestSignal, this.lifetime.signal]);
		if (method === "hello") {
			const input = helloRequest.parse(raw);
			await this.started;
			if (!this.initialized) {
				const resolved = await ctx.fs.resolve(input.workspace, { signal });
				this.workspace = ctx.fs.processPath(resolved);
				this.leaseMs = input.leaseMs;
				this.initialized = true;
			}
			const entryPath = this.options.entryPath ?? fileURLToPath(import.meta.url);
			this.hostDigest = createHash("sha256").update(await readFile(entryPath)).digest("hex");
			this.touchLease();
			return helloSchema.parse({
				protocol: 3,
				hash: this.hostDigest,
				kind: "node-script",
				platform: process.platform,
				nodeVersion: process.version,
				executable: process.execPath,
				root: this.root,
				workspace: this.workspace,
				...input.bootstrapPath === void 0 ? {} : { bootstrapHash: createHash("sha256").update(await readFile(input.bootstrapPath)).digest("hex") }
			});
		}
		await this.started;
		if (!this.initialized || this.cleanup !== void 0) throw new Error("remote workspace server is not accepting operations");
		const processes = this.processes;
		if (method === "heartbeat") {
			object.parse(raw);
			this.touchLease();
			return null;
		}
		if (method === "close") {
			if (process.env.DSH_RW_DEBUG) console.error("[srv] close handler enter");
			object.parse(raw);
			await this.release();
			if (process.env.DSH_RW_DEBUG) console.error("[srv] close released, replying");
			this.options.onClose?.();
			return null;
		}
		if (method === "process.prepare") return processes.prepare(raw);
		if (method === "process.start") return processes.start(processIdRequest.parse(raw).id, signal);
		if (method === "process.done") return processes.done(processIdRequest.parse(raw).id);
		if (method === "process.wait") return processes.wait(processIdRequest.parse(raw).id, signal);
		if (method === "process.terminate") {
			await processes.terminate(processIdRequest.parse(raw).id);
			return null;
		}
		if (method === "terminal.environment") {
			object.parse(raw);
			return ctx.subprocess.terminalEnvironment(signal);
		}
		if (method === "terminal.resize") {
			const input = resizeRequest.parse(raw);
			await processes.resizeTerminal(input.id, input.cols, input.rows);
			return null;
		}
		if (method === "terminal.write" || method === "terminal.inspect" || method === "terminal.activity" || method === "terminal.signal") {
			const input = terminalOperation.parse(raw);
			const operation = method === "terminal.write" ? "write" : method === "terminal.inspect" ? "inspect" : method === "terminal.activity" ? "activity" : "signal";
			if (operation === "signal") terminalSignalSchema.parse(input.value);
			return processes.terminal(input.id, operation, input.value);
		}
		if (method === "executable") {
			const input = executableRequest.parse(raw);
			const env = input.env === void 0 ? void 0 : Object.fromEntries(Object.entries(input.env).filter((entry) => entry[1] !== null));
			try {
				return await ctx.subprocess.resolveExecutable(input.command, env, signal);
			} catch (error) {
				if (error.name === "SubprocessExecutableNotFoundError") throw codedError(error, "SUBPROCESS_EXECUTABLE_NOT_FOUND");
				throw error;
			}
		}
		if (method === "sandbox") {
			const input = sandboxRequest.parse(raw);
			const resolved = await this.resolvePolicy(input.policy, signal);
			if (resolved.mode === "danger-full-access") throw new Error("unconfined argv does not need a sandbox wrapper");
			try {
				return await ctx.sandbox.confine(input.argv, resolved, signal);
			} catch (error) {
				if (!/no sandbox backend is usable/i.test(String(error.message))) throw error;
				noteUnconfined(resolved.mode);
				return unconfinedSandboxFacts(input.argv);
			}
		}
		if (method === "fs.resolve" || method === "fs.lstat") {
			const input = pathRequest.parse(raw);
			const cwd = input.cwd ?? this.workspace;
			return method === "fs.resolve" ? await ctx.fs.resolve(input.path, {
				cwd,
				signal
			}) : await ctx.fs.lstat(input.path, { cwd }, signal) ?? null;
		}
		if (method === "fs.stat" || method === "fs.list" || method === "fs.readText" || method === "fs.stream") {
			const target = asTarget(targetRequest.parse(raw).target);
			if (method === "fs.stat") return await ctx.fs.stat(target, signal) ?? null;
			if (method === "fs.list") return entriesSchema.parse(await ctx.fs.listDir(target, signal));
			if (method === "fs.stream") return await this.openTextStream(target, signal);
			return await this.readWholeText(target, signal);
		}
		if (method === "fs.next") {
			const id = streamIdRequest.parse(raw).id;
			const record = this.iterators.get(id);
			if (record === void 0) throw new Error("unknown remote text stream");
			const next = await record.iterator.next();
			return {
				done: next.done === true,
				value: typeof next.value === "string" ? next.value : ""
			};
		}
		if (method === "fs.streamClose") {
			const id = streamIdRequest.parse(raw).id;
			const record = this.iterators.get(id);
			if (record !== void 0) {
				this.iterators.delete(id);
				record.controller.abort(/* @__PURE__ */ new Error("remote text stream closed"));
				await record.iterator.return?.().catch(() => {});
			}
			return null;
		}
		if (method === "fs.readBytes") {
			const input = readBytesRequest.parse(raw);
			const bytes = await ctx.fs.readBytes(input.target, signal, Math.min(input.maxBytes, MAX_TEXT_BYTES));
			return Buffer.from(bytes).toString("base64");
		}
		if (method === "fs.readRange") {
			const input = readRangeRequest.parse(raw);
			if (input.length > 8388608) throw codedError(/* @__PURE__ */ new Error("remote read range exceeds the byte ceiling"), "FS_TOO_LARGE");
			const bytes = await ctx.fs.readByteRange(input.target, {
				offset: input.offset,
				length: input.length
			}, signal);
			return Buffer.from(bytes).toString("base64");
		}
		if (method === "fs.write") {
			const input = writeRequest.parse(raw);
			const resolved = await this.resolvePolicy(input.policy, signal);
			return writeResultSchema.parse(await ctx.fs.writeText(input.target, input.content, input.expected, signal, resolved));
		}
		if (method === "fs.edit") {
			const input = editRequest.parse(raw);
			const resolved = await this.resolvePolicy(input.policy, signal);
			return editResultSchema.parse(await ctx.fs.editText(input.target, input.edit, input.expected, signal, resolved));
		}
		if (method === "fs.patch") {
			const input = patchRequest.parse(raw);
			const resolved = await this.resolvePolicy(input.policy, signal);
			return editResultSchema.parse(await ctx.fs.patchText(input.target, input.hunks, input.expected, signal, resolved));
		}
		if (method === "fs.delete") {
			const input = deleteRequest.parse(raw);
			const resolved = await this.resolvePolicy(input.policy, signal);
			return deleteResultSchema.parse(await ctx.fs.deleteFile(input.target, input.expected, signal, resolved));
		}
		if (method === "fs.move") {
			const input = moveRequest.parse(raw);
			const resolved = await this.resolvePolicy(input.policy, signal);
			return moveResultSchema.parse(await ctx.fs.moveFile(input.source, input.destination, input.options ?? {}, signal, resolved));
		}
		throw new Error(`unknown remote workspace operation "${method}"`);
	}
	/** Whole-text reads go through the iterator API so the byte ceiling is enforced remotely. */
	async readWholeText(target, signal) {
		const iterator = await this.options.ctx.fs.streamText(target, signal);
		let total = 0;
		let text = "";
		try {
			for await (const chunk of iterator) {
				total += Buffer.byteLength(chunk, "utf8");
				if (total > 8388608) throw codedError(/* @__PURE__ */ new Error(`remote text exceeds ${MAX_TEXT_BYTES} bytes`), "FS_TOO_LARGE");
				text += chunk;
			}
		} finally {
			await iterator.return?.().catch(() => {});
		}
		return text;
	}
	/** Register an iterator before returning its id, so the first `fs.next` cannot race. */
	async openTextStream(target, signal) {
		if (this.iterators.size >= 128) throw new Error("remote text stream limit reached");
		const controller = new AbortController();
		const iterator = await this.options.ctx.fs.streamText(target, AbortSignal.any([signal, controller.signal]));
		const id = randomUUID();
		this.iterators.set(id, {
			iterator,
			controller
		});
		return id;
	}
};
/** Attach a machine-readable code without depending on a provider error class. */
function codedError(error, code) {
	const detail = error instanceof Error ? error : new Error(String(error));
	Object.defineProperty(detail, "code", {
		value: code,
		enumerable: true,
		configurable: true
	});
	return detail;
}
//#endregion
//#region src/server-plugin/mux.ts
/**
* Execution-world mux over WebSocket (protocol §7).
*
* Outer handshake (plugin-owned): a text `hello` / `hello-ok` exchange after
* the HTTP upgrade. After that, **one WS binary message = one
* `src/frame/codec.ts` mux frame**, exactly the bytes the retired stdio
* channel carried. The server side reuses `RemoteExecutionServer`, which
* dispatches the official helper method set against this profile's
* `ctx.fs`/`ctx.subprocess`/`ctx.sandbox` providers.
*/
const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const MAX_WS_MESSAGE_BYTES = 67108864;
/** Minimal RFC-6455 server half: masked client frames in, unmasked frames out. */
var WebSocketChannel = class WebSocketChannel {
	socket;
	handlers;
	buffer = Buffer.alloc(0);
	fragment;
	closed = false;
	constructor(socket, handlers) {
		this.socket = socket;
		this.handlers = handlers;
		socket.on("data", (chunk) => this.feed(chunk));
		socket.on("error", () => this.teardown());
		socket.on("close", () => this.teardown());
	}
	/** Complete the HTTP upgrade and start a channel; any `head` bytes are fed too. */
	static accept(req, socket, head) {
		const key = req.headers["sec-websocket-key"];
		if (typeof key !== "string" || key === "") {
			socket.destroy();
			return;
		}
		const accept = createHash("sha1").update(`${key}${WS_GUID}`).digest("base64");
		socket.write([
			"HTTP/1.1 101 Switching Protocols",
			"Upgrade: websocket",
			"Connection: Upgrade",
			`Sec-WebSocket-Accept: ${accept}`,
			"",
			""
		].join("\r\n"));
		let channel;
		channel = new WebSocketChannel(socket, {
			onText: (text) => channel.current.onText(text),
			onBinary: (bytes) => channel.current.onBinary(bytes),
			onClose: () => channel.current.onClose()
		});
		if (head.length > 0) channel.feed(head);
		return channel;
	}
	current = {
		onText: () => {
			this.close(4003, "mux hello missing");
		},
		onBinary: () => {
			this.close(4003, "mux hello missing");
		},
		onClose: () => {}
	};
	/** Replace the message handlers (used once the outer hello is accepted). */
	setHandlers(handlers) {
		this.current = handlers;
	}
	/** Send one text control frame. */
	sendText(text) {
		this.sendFrame(1, Buffer.from(text, "utf8"));
	}
	/** Send one binary mux frame. */
	sendBinary(payload) {
		this.sendFrame(2, payload);
	}
	/** Send a close frame and tear the socket down. */
	close(code = 1e3, reason = "") {
		if (this.closed) return;
		const body = Buffer.alloc(2 + Buffer.byteLength(reason));
		body.writeUInt16BE(code, 0);
		body.write(reason, 2);
		this.sendFrame(8, body);
		this.teardown();
	}
	sendFrame(opcode, payload) {
		if (this.closed) return;
		const length = payload.length;
		let header;
		if (length < 126) header = Buffer.from([128 | opcode, length]);
		else if (length < 65536) {
			header = Buffer.alloc(4);
			header[0] = 128 | opcode;
			header[1] = 126;
			header.writeUInt16BE(length, 2);
		} else {
			header = Buffer.alloc(10);
			header[0] = 128 | opcode;
			header[1] = 127;
			header.writeBigUInt64BE(BigInt(length), 2);
		}
		try {
			this.socket.write(Buffer.concat([header, payload]));
		} catch {
			this.teardown();
		}
	}
	feed(chunk) {
		if (this.closed) return;
		this.buffer = this.buffer.length === 0 ? Buffer.from(chunk) : Buffer.concat([this.buffer, chunk]);
		while (this.buffer.length >= 2) {
			const first = this.buffer[0];
			const second = this.buffer[1];
			const fin = (first & 128) !== 0;
			const opcode = first & 15;
			const masked = (second & 128) !== 0;
			let length = second & 127;
			let offset = 2;
			if (length === 126) {
				if (this.buffer.length < 4) return;
				length = this.buffer.readUInt16BE(2);
				offset = 4;
			} else if (length === 127) {
				if (this.buffer.length < 10) return;
				const big = this.buffer.readBigUInt64BE(2);
				if (big > BigInt(MAX_WS_MESSAGE_BYTES)) {
					this.close(1009, "frame too large");
					return;
				}
				length = Number(big);
				offset = 10;
			}
			if (length > MAX_WS_MESSAGE_BYTES) {
				this.close(1009, "frame too large");
				return;
			}
			const maskOffset = offset;
			if (masked) offset += 4;
			if (this.buffer.length < offset + length) return;
			let payload = this.buffer.subarray(offset, offset + length);
			if (masked) {
				const mask = this.buffer.subarray(maskOffset, maskOffset + 4);
				const unmasked = Buffer.allocUnsafe(payload.length);
				for (let i = 0; i < payload.length; i++) unmasked[i] = payload[i] ^ mask[i % 4];
				payload = unmasked;
			}
			this.buffer = this.buffer.subarray(offset + length);
			if (opcode === 8) {
				this.close(1e3);
				return;
			}
			if (opcode === 9) {
				this.sendFrame(10, payload);
				continue;
			}
			if (opcode === 10) continue;
			if (opcode === 0) {
				if (this.fragment === void 0) {
					this.close(1002, "unexpected continuation");
					return;
				}
				this.fragment.chunks.push(payload);
				if (fin) {
					const complete = Buffer.concat(this.fragment.chunks);
					const kind = this.fragment.opcode;
					this.fragment = void 0;
					if (kind === 1) this.current.onText(complete.toString("utf8"));
					else this.current.onBinary(complete);
				}
				continue;
			}
			if (opcode !== 1 && opcode !== 2) {
				this.close(1003, "unsupported opcode");
				return;
			}
			if (!fin) {
				this.fragment = {
					opcode,
					chunks: [payload]
				};
				continue;
			}
			if (opcode === 1) this.current.onText(payload.toString("utf8"));
			else this.current.onBinary(payload);
		}
	}
	teardown() {
		if (this.closed) return;
		this.closed = true;
		this.current.onClose();
		if (!this.socket.destroyed) this.socket.destroy();
	}
};
/** Parse the outer hello JSON value; throws a coded PluginError on mismatch. */
function parseMuxHelloValue(value, lease) {
	if (value === null || typeof value !== "object" || Array.isArray(value)) throw new PluginError("bad-request", "mux hello must be an object");
	const hello = value;
	if (hello["kind"] !== "hello") throw new PluginError("bad-request", "first mux frame must be { kind: \"hello\" }");
	if (Number(hello["protocol"]) !== 1) throw new PluginError("plugin-protocol-mismatch", `unsupported mux protocol ${String(hello["protocol"])}; this server speaks 1`, { serverProtocol: 1 });
	const clientId = typeof hello["clientId"] === "string" ? hello["clientId"] : "";
	const instanceId = typeof hello["instanceId"] === "string" ? hello["instanceId"] : "";
	if (!lease.matches(clientId, instanceId)) throw new PluginError("lease-mismatch", "the mux hello identity does not match the active profile lease");
	return {
		clientId,
		instanceId
	};
}
/** Parse one text hello message (compatibility path). */
function parseMuxHello(text, lease) {
	let value;
	try {
		value = JSON.parse(text);
	} catch {
		throw new PluginError("bad-request", "mux hello is not JSON");
	}
	return parseMuxHelloValue(value, lease);
}
/** Decode one outer binary hello (a FRAME_JSON codec frame); returns the payload. */
function decodeBinaryHello(bytes) {
	if (bytes.length <= 5) throw new PluginError("bad-request", "mux hello frame is empty");
	if (bytes[0] !== 0) throw new PluginError("bad-request", "first mux frame must be a FRAME_JSON hello frame");
	try {
		return JSON.parse(bytes.subarray(5).toString("utf8"));
	} catch {
		throw new PluginError("bad-request", "mux hello payload is not JSON");
	}
}
/**
* Serve one authenticated upgrade: outer hello, then FrameMux bytes to the
* execution-world server. The caller has already run `connection.admit` and
* checked the lease headers.
*/
function serveMux(ctx, lease, req, socket, head) {
	const channel = WebSocketChannel.accept(req, socket, head);
	if (channel === void 0) return;
	let server;
	let input;
	let helloSettled = false;
	const dispose = () => {
		server?.dispose().catch(() => {});
		server = void 0;
		if (input !== void 0 && !input.destroyed) input.end();
		input = void 0;
	};
	const settleHello = () => {
		helloSettled = true;
		const source = new PassThrough();
		input = source;
		const output = new Writable({
			write(chunk, _encoding, callback) {
				channel.sendBinary(chunk);
				callback();
			},
			final(callback) {
				channel.close();
				callback();
			}
		});
		try {
			server = new RemoteExecutionServer({
				ctx,
				input: source,
				output,
				workspace: process.cwd()
			});
		} catch (error) {
			channel.close(4003, error instanceof Error ? error.message : "execution world unavailable");
			return;
		}
		channel.setHandlers({
			onText: () => {
				channel.close(4002, "mux accepts binary frames only after hello");
			},
			onBinary: (bytes) => {
				if (input === void 0) {
					channel.close(4004, "mux is closing");
					return;
				}
				input.write(bytes);
			},
			onClose: dispose
		});
		channel.sendBinary(encodeJsonFrame(Buffer.from(JSON.stringify({
			kind: "hello-ok",
			protocol: 1,
			maxFrameBytes: DEFAULT_MAX_FRAME_BYTES
		}), "utf8")));
	};
	channel.setHandlers({
		onText: (text) => {
			if (helloSettled) {
				channel.close(4002, "unexpected mux text frame");
				return;
			}
			try {
				parseMuxHello(text, lease);
			} catch (error) {
				const code = error instanceof PluginError && error.code === "lease-mismatch" ? 4003 : 4001;
				channel.close(code, error instanceof Error ? error.message : "mux hello rejected");
				return;
			}
			settleHello();
		},
		onBinary: (bytes) => {
			if (helloSettled) {
				if (input === void 0) {
					channel.close(4004, "mux is closing");
					return;
				}
				input.write(bytes);
				return;
			}
			try {
				parseMuxHelloValue(decodeBinaryHello(bytes), lease);
			} catch (error) {
				const code = error instanceof PluginError && error.code === "lease-mismatch" ? 4003 : 4001;
				channel.close(code, error instanceof Error ? error.message : "mux hello rejected");
				return;
			}
			settleHello();
		},
		onClose: dispose
	});
}
/** Per-entry text ceilings; tool output is commonly larger than a UI card. */
const TIMELINE_MAX_TEXT_CHARS = 16384;
const TIMELINE_MAX_ARGUMENT_CHARS = 8192;
function asRecord$1(value) {
	return value !== null && typeof value === "object" && !Array.isArray(value) ? value : {};
}
function asNumber(value, fallback) {
	return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : fallback;
}
function bounded(text, max) {
	const totalBytes = Buffer.byteLength(text, "utf8");
	if (text.length <= max) return {
		text,
		truncated: false,
		totalBytes
	};
	return {
		text: `${text.slice(0, max)}\n… [truncated ${text.length - max} chars]`,
		truncated: true,
		totalBytes
	};
}
function blockText(content) {
	if (!Array.isArray(content)) return {
		text: "",
		reasoning: ""
	};
	let text = "";
	let reasoning = "";
	for (const block of content) {
		const value = asRecord$1(block);
		if (value["type"] === "text" && typeof value["text"] === "string") text += value["text"];
		else if (value["type"] === "reasoning" && typeof value["text"] === "string") reasoning += value["text"];
	}
	return {
		text,
		reasoning
	};
}
/** Stable across pages/connections: event seq + kind (+ stable sub-kind). */
function entryId(seq, kind, suffix = "") {
	return `${seq}:${kind}${suffix === "" ? "" : `:${suffix}`}`;
}
/**
* Normalize one event into zero or more timeline rows. This is pure and
* tolerant: an event type unknown to this build becomes one `unknown` row
* instead of failing the page.
*/
function normalizeEvent(event) {
	const seq = asNumber(event.seq, 0);
	const time = asNumber(event.time, 0);
	const data = asRecord$1(event.data);
	const type = typeof event.type === "string" ? event.type : "unknown";
	const make = (kind, suffix = "") => ({
		entryId: entryId(seq, kind, suffix),
		seq,
		time,
		kind
	});
	switch (type) {
		case "user/message": {
			if (asRecord$1(data["source"])["kind"] !== "user") return [];
			const body = blockText(data["content"]);
			return [{
				...make("user", ""),
				text: body.text,
				...typeof data["id"] === "string" ? { messageId: data["id"] } : {}
			}];
		}
		case "assistant/message": {
			const body = blockText(asRecord$1(data["message"])["content"]);
			const rows = [];
			if (body.reasoning !== "") rows.push({
				...make("reasoning"),
				text: bounded(body.reasoning, TIMELINE_MAX_TEXT_CHARS).text
			});
			rows.push({
				...make("assistant", ""),
				text: bounded(body.text, TIMELINE_MAX_TEXT_CHARS).text,
				...data["interrupted"] === true ? { interrupted: true } : {}
			});
			return rows;
		}
		case "tool/call": {
			const args = typeof data["arguments"] === "string" ? data["arguments"] : "";
			return [{
				...make("tool-call", ""),
				callId: String(data["callId"] ?? ""),
				name: String(data["name"] ?? ""),
				arguments: bounded(args, TIMELINE_MAX_ARGUMENT_CHARS).text
			}];
		}
		case "tool/result": {
			const message = asRecord$1(data["message"]);
			const limited = bounded(blockText(message["content"]).text, TIMELINE_MAX_TEXT_CHARS);
			const error = asRecord$1(data["error"]);
			return [{
				...make("tool-result", ""),
				callId: String(message["toolCallId"] ?? ""),
				isError: message["isError"] === true || Object.keys(error).length > 0,
				text: limited.text,
				...limited.truncated ? {
					truncated: true,
					totalBytes: limited.totalBytes
				} : {},
				...Object.keys(error).length === 0 ? {} : { error: {
					...typeof error["name"] === "string" ? { name: error["name"] } : {},
					...typeof error["code"] === "string" ? { code: error["code"] } : {},
					...typeof error["reason"] === "string" ? { reason: error["reason"] } : {}
				} }
			}];
		}
		case "turn/start": return [{
			...make("turn", "start"),
			phase: "start",
			turn: asNumber(data["turn"], 0)
		}];
		case "turn/end": {
			const reason = data["reason"];
			return [{
				...make("turn", "end"),
				phase: "end",
				turn: asNumber(data["turn"], 0),
				...reason === void 0 ? {} : { reason: typeof reason === "string" ? reason : JSON.stringify(reason) }
			}];
		}
		case "assistant/attempt": {
			const stream = Array.isArray(data["stream"]) ? data["stream"] : [];
			let failure = {};
			for (const record of stream) {
				const chunk = asRecord$1(asRecord$1(record)["chunk"]);
				if (chunk["type"] !== "finish") continue;
				const reason = asRecord$1(chunk["reason"]);
				if (reason["kind"] === "error" || reason["kind"] === "aborted") failure = asRecord$1(reason["failure"]);
			}
			return [{
				...make("error"),
				message: typeof failure["message"] === "string" ? failure["message"] : "assistant attempt failed",
				...typeof failure["code"] === "string" ? { code: failure["code"] } : {}
			}];
		}
		case "approval/asked": return [{
			...make("approval", "asked"),
			approvalId: String(data["id"] ?? ""),
			state: "asked",
			toolName: String(data["toolName"] ?? ""),
			...typeof data["reason"] === "string" ? { reason: data["reason"] } : {}
		}];
		case "approval/decided": return [{
			...make("approval", "decided"),
			approvalId: String(data["id"] ?? ""),
			state: "decided",
			...typeof data["outcome"] === "string" ? { outcome: data["outcome"] } : {}
		}];
		case "agent/inbox/spliced":
		case "model/selection":
		case "plan/mode":
		case "working-directory/change":
		case "workspace/changes":
		case "session/title-llm-request":
		case "step/start":
		case "step/end":
		case "system/message":
		case "developer/message":
		case "request/header":
		case "request/context":
		case "session/end-seed":
		case "session/title":
		case "agent-preset/selected":
		case "permission/preset":
		case "sandbox/mode":
		case "approval/policy": return [];
		default: return [{
			...make("unknown", ""),
			type,
			ignorable: event.ignorable === true
		}];
	}
}
/** Normalize an ascending event slice. */
function normalizeTimeline(events) {
	const rows = [];
	for (const event of events) rows.push(...normalizeEvent(event));
	return rows;
}
/** Open errors get the stable not-found vocabulary. */
function openError(sessionId, error) {
	const name = error?.name;
	if (name === "SessionNotFoundError" || name === "SessionPersistenceNotFoundError") throw new PluginError("session-not-found", `session "${sessionId}" does not exist`, { sessionId });
	throw error;
}
/** Read one raw event slice from an already-open handle. */
async function slice(handle, offset, length) {
	if (length <= 0) return [];
	return await (await handle.read(offset, length)).events;
}
/** Walk backward until `limit` visible entries are collected (or seq 0). */
async function backwardEntries(handle, upper, limit) {
	let cursor = upper;
	let collected = [];
	while (cursor > 0 && collected.length < limit) {
		const start = Math.max(0, cursor - 64);
		collected = [...normalizeTimeline(await slice(handle, start, cursor - start)), ...collected];
		cursor = start;
	}
	return collected.slice(-limit);
}
/** Walk forward until `limit` visible entries are collected (or end of log). */
async function forwardEntries(handle, lower, upper, limit) {
	let cursor = lower;
	let collected = [];
	while (cursor < upper && collected.length < limit) {
		const events = await slice(handle, cursor, Math.min(64, upper - cursor));
		collected.push(...normalizeTimeline(events));
		cursor += events.length;
		if (events.length === 0) break;
	}
	return collected.slice(0, limit);
}
/**
* Read one page of normalized entries. `beforeSeq` (exclusive upper cursor)
* returns the newest older page; `afterSeq` (exclusive lower cursor) returns
* the oldest newer page. Paging counts normalized entries, so ignored log
* events never consume the caller's limit. `nextBeforeSeq`/`nextAfterSeq`
* are the oldest/newest returned entry seqs.
*/
async function readTimeline(ctx, params) {
	const sessionId = typeof params["sessionId"] === "string" ? params["sessionId"] : "";
	if (sessionId === "") throw new PluginError("bad-request", "session/timeline requires sessionId");
	const limitRaw = typeof params["limit"] === "number" ? params["limit"] : 50;
	const limit = Math.max(1, Math.min(200, Number.isFinite(limitRaw) ? Math.trunc(limitRaw) : 50));
	const persistence = ctx.get("sessionPersistence");
	if (persistence === void 0) throw new PluginError("internal", "the profile has no sessionPersistence service");
	let handle;
	try {
		handle = await persistence.open(sessionId, "read");
	} catch (error) {
		openError(sessionId, error);
	}
	try {
		let eventCount = (await persistence.stat?.(sessionId).catch(() => void 0))?.eventCount;
		if (eventCount === void 0) eventCount = (await handle.read(0, Number.MAX_SAFE_INTEGER)).events.length;
		const afterRaw = params["afterSeq"];
		const beforeRaw = params["beforeSeq"];
		if (typeof afterRaw === "number" && Number.isSafeInteger(afterRaw) && afterRaw >= 0) {
			const entries = await forwardEntries(handle, Math.min(afterRaw, eventCount), eventCount, limit);
			return {
				sessionId,
				entries,
				eventCount,
				beforeSeq: eventCount,
				nextBeforeSeq: entries[0]?.seq ?? null,
				afterSeq: afterRaw,
				nextAfterSeq: entries.at(-1)?.seq ?? null
			};
		}
		const upper = Math.max(0, Math.min(eventCount, typeof beforeRaw === "number" && Number.isSafeInteger(beforeRaw) ? beforeRaw : eventCount));
		const entries = await backwardEntries(handle, upper, limit);
		return {
			sessionId,
			entries,
			eventCount,
			beforeSeq: upper,
			nextBeforeSeq: entries[0]?.seq ?? null,
			afterSeq: -1,
			nextAfterSeq: entries.at(-1)?.seq ?? null
		};
	} finally {
		await handle.close().catch(() => {});
	}
}
/** Parse the query string without trusting downstream decoding. */
function sessionEventParams(url) {
	const query = new URL(url ?? "/", "http://127.0.0.1").searchParams;
	const sessionId = query.get("sessionId") ?? "";
	if (sessionId === "") throw new PluginError("bad-request", "session/events requires sessionId");
	const raw = query.get("afterSeq");
	const afterSeq = raw === null || raw === "" ? -1 : Number(raw);
	if (!Number.isSafeInteger(afterSeq) || afterSeq < -1) throw new PluginError("bad-request", "afterSeq must be -1 or a non-negative integer");
	return {
		sessionId,
		afterSeq
	};
}
/** Sleep that resolves early when the stream is aborted. */
function delay(ms, signal) {
	return new Promise((resolve) => {
		if (signal.aborted) {
			resolve();
			return;
		}
		const timer = setTimeout(done, ms);
		timer.unref();
		function done() {
			clearTimeout(timer);
			signal.removeEventListener("abort", done);
			resolve();
		}
		signal.addEventListener("abort", done, { once: true });
	});
}
/**
* Run one SSE session stream until the client disconnects or the profile
* unloads. `afterSeq` is the last committed ordinary event seq the client
* already rendered; `-1` starts from the first event.
*/
async function streamSessionEvents(ctx, options) {
	const persistence = ctx.get("sessionPersistence");
	if (persistence === void 0) throw new PluginError("internal", "the profile has no sessionPersistence service");
	let handle;
	try {
		handle = await persistence.open(options.sessionId, "read");
	} catch (error) {
		const name = error?.name;
		if (name === "SessionNotFoundError" || name === "SessionPersistenceNotFoundError") throw new PluginError("session-not-found", `session "${options.sessionId}" does not exist`, { sessionId: options.sessionId });
		throw error;
	}
	const unsubscribe = options.approvals?.subscribe(options.sessionId, (approval) => {
		if (!options.sink.closed()) options.sink.send({
			event: "approval",
			data: approval
		});
	});
	let cursor = options.afterSeq;
	let lastStatus = "";
	let lastHeartbeat = Date.now();
	try {
		let initialCount = (await persistence.stat?.(options.sessionId).catch(() => void 0))?.eventCount;
		if (initialCount === void 0) initialCount = (await handle.read(0, Number.MAX_SAFE_INTEGER)).events.length;
		options.sink.send({
			event: "ready",
			data: {
				sessionId: options.sessionId,
				afterSeq: cursor,
				eventCount: initialCount
			}
		});
		let idlePolls = 0;
		while (!options.signal.aborted && !options.sink.closed()) {
			const offset = cursor + 1;
			const slice = await handle.read(offset, 200);
			if (slice.events.length > 0) {
				const entries = normalizeTimeline(slice.events).filter((entry) => entry.seq > options.afterSeq);
				cursor = offset + slice.events.length - 1;
				if (entries.length > 0) {
					const count = await persistence.stat?.(options.sessionId).then((value) => value?.eventCount, () => void 0);
					options.sink.send({
						event: "timeline",
						data: {
							sessionId: options.sessionId,
							entries,
							afterSeq: cursor,
							eventCount: count ?? cursor + 1
						}
					});
				}
				idlePolls = 0;
			} else idlePolls += 1;
			if (idlePolls > 0 && idlePolls % 8 === 0) {
				const status = sessionStatus(ctx, options.sessionId);
				const serialized = JSON.stringify(status);
				if (serialized !== lastStatus) {
					lastStatus = serialized;
					options.sink.send({
						event: "status",
						data: status
					});
				}
			}
			if (Date.now() - lastHeartbeat >= 15e3) {
				lastHeartbeat = Date.now();
				options.sink.comment?.("ping");
			}
			await delay(250, options.signal);
		}
	} finally {
		unsubscribe?.();
		await handle.close().catch(() => {});
	}
}
/** Status frame body; kept here so the SSE loop and `session/status` agree. */
function sessionStatus(ctx, sessionId, pending = []) {
	const agent = ctx.get("agents")?.get(sessionId);
	const status = typeof agent?.status === "string" ? agent.status : void 0;
	return {
		sessionId,
		agentAvailable: agent !== void 0,
		...status === void 0 ? {} : { status },
		running: status === "running",
		pendingApprovals: pending
	};
}
//#endregion
//#region src/server-plugin/agent-control.ts
/**
* v2 agent control: prompt/cancel/approval without `sessionController.create`.
*
* The live `ctx.agents` registry is the only requirement. A cold session is
* resumed through the plugin's existing managed-handle path (or reported as a
* coded `agent-unavailable`), so a profile that lacks `sessionController` can
* still drive a blank/server session.
*/
/** Pending approval requests keyed by service-issued id; also an SSE source. */
var ApprovalHub = class {
	pending = /* @__PURE__ */ new Map();
	listeners = /* @__PURE__ */ new Map();
	disposed = false;
	/** Whether a v2 SSE client is watching this session (it can answer approvals). */
	hasSubscribers(sessionId) {
		return (this.listeners.get(sessionId)?.size ?? 0) > 0;
	}
	/**
	* Answer an approval request. Without a v2 watcher (and without the QA
	* marker) earlier waterfall listeners keep priority; a v2 SSE subscriber or
	* the QA gate preempts them so the self-drawn client can answer instead of a
	* background official-UI answerer holding the request forever.
	*/
	async handle(request, next, preempt = false) {
		if (this.disposed) return "unavailable";
		const sessionId = String(request.agent.id ?? "");
		if (sessionId === "") return "unavailable";
		if (preempt || this.hasSubscribers(sessionId) || process.env.DSH_RW_DEBUG === "1") console.error(`[dsh-remote-workspace-server] approval request session=${sessionId} preempt=${preempt} subscribers=${this.hasSubscribers(sessionId)} pending=${this.pending.size}`);
		if (!preempt && !this.hasSubscribers(sessionId)) {
			const earlier = await next();
			if (earlier !== "unavailable") return earlier;
		}
		const approvalId = randomUUID();
		const summary = {
			approvalId,
			sessionId,
			toolName: request.toolName,
			...request.callId === void 0 ? {} : { callId: String(request.callId) },
			...request.reason === void 0 ? {} : { reason: request.reason },
			...request.displayReason === void 0 ? {} : { displayReason: request.displayReason },
			requestedAt: Date.now()
		};
		return await new Promise((resolve) => {
			const onAbort = () => {
				finish("cancelled");
			};
			const finish = (value) => {
				const entry = this.pending.get(approvalId);
				if (entry === void 0) return;
				this.pending.delete(approvalId);
				clearTimeout(entry.timer);
				entry.signal?.removeEventListener("abort", onAbort);
				resolve(value);
			};
			const timer = setTimeout(() => {
				finish("unavailable");
			}, 12e4);
			timer.unref();
			const entry = {
				summary,
				resolve: finish,
				...request.signal === void 0 ? {} : { signal: request.signal },
				...request.signal === void 0 ? {} : { onAbort },
				timer
			};
			this.pending.set(approvalId, entry);
			request.signal?.addEventListener("abort", onAbort, { once: true });
			if (request.signal?.aborted === true) finish("cancelled");
			this.notify(summary);
		});
	}
	/** Resolve one pending request from `session/approval`. */
	answer(params) {
		const sessionId = typeof params["sessionId"] === "string" ? params["sessionId"] : "";
		const approvalId = typeof params["approvalId"] === "string" ? params["approvalId"] : "";
		const decision = params["decision"];
		if (sessionId === "" || approvalId === "") throw new PluginError("bad-request", "session/approval requires sessionId and approvalId");
		if (decision !== "allowed-once" && decision !== "rejected" && decision !== "cancelled" && decision !== "allow" && decision !== "deny") throw new PluginError("bad-request", "session/approval decision must be allowed-once, rejected or cancelled");
		const entry = this.pending.get(approvalId);
		if (entry === void 0 || entry.summary.sessionId !== sessionId) throw new PluginError("bad-request", `approval "${approvalId}" is not pending for session "${sessionId}"`, {
			approvalId,
			sessionId
		});
		const normalized = decision === "allow" ? "allowed-once" : decision === "deny" ? "rejected" : decision;
		entry.resolve(normalized);
		return {
			accepted: true,
			approvalId,
			decision: normalized
		};
	}
	/** Pending summaries for status/SSE. */
	list(sessionId) {
		return [...this.pending.values()].map((entry) => entry.summary).filter((summary) => summary.sessionId === sessionId);
	}
	subscribe(sessionId, listener) {
		const set = this.listeners.get(sessionId) ?? /* @__PURE__ */ new Set();
		set.add(listener);
		this.listeners.set(sessionId, set);
		return () => {
			const current = this.listeners.get(sessionId);
			if (current === void 0) return;
			current.delete(listener);
			if (current.size === 0) this.listeners.delete(sessionId);
		};
	}
	/**
	* Register the approval listener on an agent-scoped context. The official
	* service dispatches with `scopeTarget(agent, agent)`; an agent-scoped
	* listener is admitted on the real profile, while a root plugin listener is
	* filtered out.
	*/
	attachAgent(agentCtx, preempt = false) {
		return installApprovalAnswerer(agentCtx, this, preempt);
	}
	/** Resolve any outstanding request as cancelled and stop accepting new ones. */
	dispose() {
		this.disposed = true;
		for (const entry of [...this.pending.values()]) entry.resolve("cancelled");
		this.pending.clear();
		this.listeners.clear();
	}
	notify(summary) {
		for (const listener of this.listeners.get(summary.sessionId) ?? []) try {
			listener(summary);
		} catch {}
	}
};
/** Install the approval waterfall listener; returns its disposer. */
function installApprovalAnswerer(ctx, hub, preempt = false) {
	const off = ctx.on("approval/request", (request, next) => hub.handle(request, next, preempt), { prepend: true });
	return () => {
		off?.();
	};
}
function outgoingMessage(content) {
	return {
		id: randomUUID(),
		role: "user",
		content,
		source: { kind: "user" }
	};
}
const MAX_ATTACHMENTS = 4;
const MAX_ATTACHMENT_BYTES = 614400;
const MAX_ATTACHMENTS_BYTES = 921600;
/** Decode one canonical base64 attachment and enforce the host-route budget. */
function decodeAttachment(value) {
	if (typeof value !== "object" || value === null) throw new PluginError("bad-request", "attachment must be an object");
	const input = value;
	const name = typeof input["name"] === "string" ? input["name"] : "";
	const mediaType = typeof input["mediaType"] === "string" ? input["mediaType"] : "application/octet-stream";
	const data = typeof input["data"] === "string" ? input["data"] : "";
	if (name === "" || data === "") throw new PluginError("bad-request", "attachment requires name and data");
	if (data.length > Math.ceil(MAX_ATTACHMENT_BYTES / 3) * 4 + 4) throw new PluginError("bad-request", `attachment "${name}" is too large`);
	const bytes = Buffer.from(data, "base64");
	if (bytes.byteLength === 0 || bytes.byteLength > MAX_ATTACHMENT_BYTES) throw new PluginError("bad-request", `attachment "${name}" is too large`);
	return {
		name,
		mediaType,
		bytes
	};
}
/** Build the prompt parts, admitting uploads through the server attachment store. */
async function promptContent(ctx, text, raw) {
	const content = [{
		type: "text",
		text
	}];
	if (raw === void 0) return {
		content,
		bytes: 0
	};
	if (!Array.isArray(raw)) throw new PluginError("bad-request", "attachments must be an array");
	if (raw.length > MAX_ATTACHMENTS) throw new PluginError("bad-request", `at most ${MAX_ATTACHMENTS} attachments are allowed`);
	const decoded = raw.map(decodeAttachment);
	const bytes = decoded.reduce((sum, item) => sum + item.bytes.byteLength, 0);
	if (bytes > MAX_ATTACHMENTS_BYTES) throw new PluginError("bad-request", "attachments exceed the size budget");
	if (decoded.length === 0) return {
		content,
		bytes
	};
	const attachments = ctx.get("attachments");
	if (attachments === void 0) throw new PluginError("bad-request", "the profile has no attachment store");
	for (const item of decoded) if (item.mediaType.startsWith("image/")) {
		const [ref] = await attachments.saveImages([{
			data: item.bytes,
			mediaType: item.mediaType,
			name: item.name
		}]);
		content.push({
			type: "image",
			attachment: ref
		});
	} else {
		const ref = await attachments.admitEncodedFile({
			data: item.bytes,
			name: item.name
		});
		content.push({
			type: "file",
			attachment: ref
		});
	}
	return {
		content,
		bytes
	};
}
/** Resolve a live agent, resuming a cold server session through the managed handle. */
async function ensureAgent(ctx, managed, sessionId) {
	const agents = ctx.get("agents");
	if (agents === void 0) throw new PluginError("agent-unavailable", "the profile has no agents service", { sessionId });
	const existing = agents.get(sessionId);
	if (existing !== void 0) return existing;
	if (managed === void 0) throw new PluginError("agent-unavailable", "no managed agent path is available for this session", { sessionId });
	const prepared = await managed.prepare(sessionId);
	if (prepared.managed === false) throw new PluginError("agent-unavailable", `the session cannot be driven by this plugin: ${prepared.reason}`, {
		sessionId,
		reason: prepared.reason
	});
	const resumed = agents.get(sessionId);
	if (resumed === void 0) throw new PluginError("agent-unavailable", "the agent did not become live after resume", { sessionId });
	return resumed;
}
/** `session/prompt`: deliver one follow-up or steering user message. */
async function promptSession(ctx, managed, params) {
	const sessionId = typeof params["sessionId"] === "string" ? params["sessionId"] : "";
	const text = (typeof params["text"] === "string" ? params["text"] : "").trim();
	if (sessionId === "") throw new PluginError("bad-request", "session/prompt requires sessionId");
	if (text === "") throw new PluginError("bad-request", "session/prompt requires non-empty text");
	const mode = params["mode"] === "steer" ? "steer" : "followup";
	const agent = await ensureAgent(ctx, managed, sessionId);
	const message = outgoingMessage((await promptContent(ctx, text, params["attachments"])).content);
	try {
		if (mode === "steer") agent.steer?.(message);
		else agent.followup?.(message);
	} catch (error) {
		throw new PluginError("persist-read-only", `prompt rejected: ${error instanceof Error ? error.message : String(error)}`);
	}
	return {
		accepted: true,
		sessionId
	};
}
/** `session/commands`: official command descriptors available to the live agent. */
async function listSessionCommands(ctx, managed, params) {
	const sessionId = typeof params["sessionId"] === "string" ? params["sessionId"] : "";
	if (sessionId === "") throw new PluginError("bad-request", "session/commands requires sessionId");
	const agent = await ensureAgent(ctx, managed, sessionId);
	const commands = ctx.get("commands");
	if (commands === void 0) throw new PluginError("agent-unavailable", "the profile has no commands service", { sessionId });
	return {
		sessionId,
		commands: commands.list(agent)
	};
}
/** `session/command`: execute one official slash command against the live agent. */
async function executeSessionCommand(ctx, managed, params) {
	const sessionId = typeof params["sessionId"] === "string" ? params["sessionId"] : "";
	const line = typeof params["line"] === "string" ? params["line"].trim() : "";
	if (sessionId === "" || line === "") throw new PluginError("bad-request", "session/command requires sessionId and line");
	if (!line.startsWith("/")) throw new PluginError("bad-request", "session/command line must start with /");
	const agent = await ensureAgent(ctx, managed, sessionId);
	const commands = ctx.get("commands");
	if (commands === void 0) throw new PluginError("agent-unavailable", "the profile has no commands service", { sessionId });
	const executed = await commands.execute(agent, line, [], new AbortController().signal);
	if (executed === void 0) throw new PluginError("bad-request", `unknown command "${line}"`, { sessionId });
	return {
		sessionId,
		executed: true,
		commandId: executed.commandId,
		result: executed.result
	};
}
/** `session/status`: live agent + pending approvals. */
async function sessionStatusValue(ctx, hub, params, approvalGate) {
	const sessionId = typeof params["sessionId"] === "string" ? params["sessionId"] : "";
	if (sessionId === "") throw new PluginError("bad-request", "session/status requires sessionId");
	if (ctx.get("agents")?.get(sessionId) === void 0) {
		if (await ctx.get("sessionPersistence")?.stat?.(sessionId).catch(() => void 0) === void 0) throw new PluginError("session-not-found", `session "${sessionId}" does not exist`, { sessionId });
	}
	return {
		...sessionStatus(ctx, sessionId, hub.list(sessionId)),
		...approvalGate === void 0 ? {} : { approvalGate }
	};
}
/** `session/cancel` for v2; kept here so the prompt and cancel paths share lookup. */
async function cancelSession(ctx, managed, params) {
	const sessionId = typeof params["sessionId"] === "string" ? params["sessionId"] : "";
	if (sessionId === "") throw new PluginError("bad-request", "session/cancel requires sessionId");
	(await ensureAgent(ctx, managed, sessionId)).cancel?.({ kind: "user" });
	return {
		canceled: true,
		sessionId
	};
}
/** Validate one `session/approval` payload and resolve it. */
function approveSession(hub, params) {
	return hub.answer(params);
}
//#endregion
//#region src/server-plugin/qa-approval.ts
/**
* QA-only deterministic approval gate.
*
* Disabled unless the per-client state directory contains
* `remote-workspace/<clientId>/qa-approval-tool` with a tool name (or `*`).
* When enabled, the named tool's `tools/pre-execute` waterfall returns `ask`
* before dispatch, so the official tool registry + ApprovalService produce a
* real `approval/asked` / `approval/decided` pair and the v2 hub has a pending
* approval to hold. This file is intentionally absent from package defaults;
* normal deployments never see different behavior.
*/
/**
* Read the QA gate marker; a missing/blank/bad file disables the gate.
* @param path - marker file path inside the per-client state directory.
* @returns the tool name, `*`, or undefined.
*/
function readQaApprovalTool(path) {
	try {
		const value = readFileSync(path, "utf8").trim();
		if (value === "" || value.length > 128 || /[\u0000-\u001f]/u.test(value)) return void 0;
		return value;
	} catch {
		return;
	}
}
/** Install the QA-only pre-execute gate; no marker means no listener. */
function installQaApprovalGate(ctx, tool) {
	if (tool === void 0) return () => {};
	const off = ctx.on.call(ctx, "tools/pre-execute", async (exec, next) => {
		if (tool !== "*" && exec.name !== tool) return await next();
		return {
			kind: "ask",
			reason: `QA approval probe for ${String(exec.name ?? "tool")}`,
			displayReason: {
				en: `Allow the QA approval probe to run "${String(exec.name ?? "tool")}" once?`,
				zh: `允许 QA 审批测试执行一次“${String(exec.name ?? "tool")}”？`
			}
		};
	});
	return () => {
		off?.();
	};
}
//#endregion
//#region src/server-plugin/fs-api.ts
/** P1 directory browser backing the client's "add workspace" dialog (protocol §4.3). */
/** One blocking page: official `ctx.fs.resolve` + `listDir`, no second path protocol. */
async function listDirectory(ctx, params) {
	const fs = ctx.get("fs");
	if (fs === void 0) throw new PluginError("fs-error", "the profile has no fs service");
	const home = homedir();
	const requested = typeof params["path"] === "string" && params["path"] !== "" ? params["path"] : home;
	try {
		const target = await fs.resolve(requested, { cwd: home });
		const path = fs.processPath(target);
		const entries = (await fs.listDir(target)).map((entry) => ({
			name: entry.name,
			type: entry.type === "file" || entry.type === "directory" ? entry.type : "other"
		})).sort((left, right) => {
			if (left.type !== right.type) {
				if (left.type === "directory") return -1;
				if (right.type === "directory") return 1;
			}
			return left.name.localeCompare(right.name);
		});
		return {
			home,
			path,
			parent: path === "/" ? null : posix.dirname(path),
			entries
		};
	} catch (error) {
		if (error instanceof PluginError) throw error;
		throw new PluginError("fs-error", error instanceof Error ? error.message : String(error), { path: requested });
	}
}
//#endregion
//#region src/server-plugin/session-row.ts
/** Execution end for one session row; a missing record is the historical server mode. */
function executorModeOf(record) {
	return record?.mode === "local" ? "local" : "server";
}
//#endregion
//#region src/server-plugin/persist.ts
/**
* P1 persist bridge over the official session persistence (protocol §16.7).
*
* The plugin owns only in-memory handle ids; log format, contiguity, flock and
* durability stay with the official JSONL backend. Write opens additionally
* check the executor record so a local-mode writer cannot race a server agent.
*/
const OWNER_TTL_MS = 2e4;
/** One executor error for the write-open guard table (protocol §16.5). */
function modeConflict(mode, extra = {}) {
	throw new PluginError("executor-mode-conflict", `session executor is "${mode}"; switch the executor before opening it for write`, {
		mode,
		...extra
	});
}
/** Map one official persistence failure onto the stable P1 vocabulary. */
function mapPersistenceError(error) {
	const name = error?.name;
	if (name === "SessionAlreadyExistsError") throw new PluginError("persist-create-exists", "a session with this id already exists", { sessionId: error.sessionId });
	if (name === "SessionAlreadyOwnedError") throw new PluginError("session/writer-held", error.message, { sessionId: error.sessionId });
	if (name === "SessionPersistenceNotFoundError") throw new PluginError("session-not-found", error.message);
	if (name === "SessionHandleClosedError") throw new PluginError("handle-not-found", "the session handle is closed");
	if (name === "SessionReadOnlyError") throw new PluginError("persist-read-only", "this handle is read-only");
	if (error instanceof Error && /append seq mismatch/i.test(error.message)) throw new PluginError("bad-request", error.message, { reason: "seq-mismatch" });
	throw error;
}
/** In-memory handle table plus the P1 persist methods. */
var PersistBridge = class {
	ctx;
	executor;
	ownerOf;
	cache;
	handles = /* @__PURE__ */ new Map();
	constructor(ctx, executor, ownerOf, cache = new SessionLogCache()) {
		this.ctx = ctx;
		this.executor = executor;
		this.ownerOf = ownerOf;
		this.cache = cache;
	}
	persistence() {
		const persistence = this.ctx.get("sessionPersistence");
		if (persistence === void 0) throw new PluginError("internal", "the profile has no sessionPersistence service");
		return persistence;
	}
	workspacePath(path) {
		return this.ctx.get("workspaceRegistry")?.list().some((workspace) => workspace.path === path) === true;
	}
	require(handleId) {
		const record = this.handles.get(handleId);
		if (record === void 0) throw new PluginError("handle-not-found", `unknown persist handle "${handleId}"`);
		return record;
	}
	/** `persist/create`: validate remoteCwd, create through the official backend, record executor local. */
	async create(params) {
		const header = params["header"];
		const remoteCwd = params["remoteCwd"];
		if (typeof remoteCwd !== "string" || !remoteCwd.startsWith("/")) throw new PluginError("bad-request", "persist/create requires an absolute remoteCwd");
		if (!this.workspacePath(remoteCwd)) throw new PluginError("workspace-path-invalid", `remoteCwd is not an official workspace path: ${remoteCwd}`, { remoteCwd });
		if (header === null || typeof header !== "object" || Array.isArray(header)) throw new PluginError("bad-request", "persist/create requires a header object");
		const inherited = params["inheritedEventCount"];
		const inheritedEventCount = typeof inherited === "number" && Number.isSafeInteger(inherited) && inherited >= 0 ? inherited : void 0;
		const executorValue = params["executor"];
		const sessionId = typeof header["id"] === "string" ? String(header["id"]) : "";
		try {
			const handle = await this.persistence().create({
				...header,
				cwd: remoteCwd
			}, inheritedEventCount === void 0 ? void 0 : { inheritedEventCount });
			const workspace = this.ctx.get("workspaceRegistry")?.list().find((candidate) => candidate.path === remoteCwd);
			try {
				await workspace?.attachSession(sessionId);
			} catch (error) {
				await handle.close().catch(() => {});
				throw new PluginError("internal", `session "${sessionId}" was created but could not attach to workspace: ${error instanceof Error ? error.message : String(error)}`);
			}
			const handleId = randomUUID();
			this.handles.set(handleId, {
				handle,
				sessionId,
				access: "write"
			});
			if (executorValue === "local") await this.executor.assign(sessionId, "local", this.ownerOf(), "handoff");
			return {
				handleId,
				header: {
					...handle.header,
					cwd: remoteCwd
				},
				nextSeq: inheritedEventCount ?? 0
			};
		} catch (error) {
			mapPersistenceError(error);
		}
	}
	/**
	* Exact event count for a handle.
	*
	* The shipped JSONL `stat`/`list` snapshots carry `sizeBytes` but no cheap
	* `eventCount`, so the count comes from one official `read(0)` decode. The
	* backend memoizes that decode per artifact revision; body reads stay paged.
	*/
	async eventCountOf(sessionId, handle, snapshotEventCount, sizeBytes) {
		if (typeof snapshotEventCount === "number" && Number.isSafeInteger(snapshotEventCount)) return snapshotEventCount;
		if (sizeBytes !== void 0) {
			const cached = this.cache.get(sessionId, sizeBytes);
			if (cached?.eventCount !== void 0) return cached.eventCount;
			this.cache.allowDecode(sessionId, sizeBytes);
		}
		const { events } = await handle.read(0);
		if (sizeBytes !== void 0) this.cache.rememberCount(sessionId, sizeBytes, events.length, foldFacts(events));
		return events.length;
	}
	/** `persist/open`: read opens freely; write opens pass the executor guard first. */
	async open(params) {
		const sessionId = typeof params["sessionId"] === "string" ? params["sessionId"] : "";
		const access = params["access"] === "write" ? "write" : params["access"] === "read" ? "read" : void 0;
		if (sessionId === "" || access === void 0) throw new PluginError("bad-request", "persist/open requires sessionId and access");
		if (access === "write") await this.guardWriteOpen(sessionId);
		try {
			const handle = await this.persistence().open(sessionId, access);
			const stored = await this.persistence().stat(sessionId).catch(() => void 0);
			const eventCount = await this.eventCountOf(sessionId, handle, stored?.eventCount, stored?.sizeBytes);
			const handleId = randomUUID();
			this.handles.set(handleId, {
				handle,
				sessionId,
				access
			});
			return {
				handleId,
				header: handle.header,
				inheritedEventCount: Number(handle.inheritedEventCount),
				eventCount
			};
		} catch (error) {
			mapPersistenceError(error);
		}
	}
	/** `persist/read` with offset/length paging and nextOffset. */
	async read(params) {
		const record = this.require(typeof params["handleId"] === "string" ? params["handleId"] : "");
		const offset = typeof params["offset"] === "number" && Number.isSafeInteger(params["offset"]) && params["offset"] >= 0 ? params["offset"] : 0;
		const length = typeof params["length"] === "number" && Number.isSafeInteger(params["length"]) && params["length"] >= 0 ? params["length"] : void 0;
		try {
			const result = await record.handle.read(offset, length);
			return {
				events: result.events,
				eventState: result.eventState,
				nextOffset: offset + result.events.length
			};
		} catch (error) {
			mapPersistenceError(error);
			if (error?.name === "SessionHandleClosedError") throw new PluginError("handle-not-found", "the session handle is closed");
			throw error;
		}
	}
	async append(params) {
		const record = this.require(typeof params["handleId"] === "string" ? params["handleId"] : "");
		if (record.access !== "write") throw new PluginError("persist-read-only", "this handle is read-only");
		const events = Array.isArray(params["events"]) ? params["events"] : void 0;
		if (events === void 0) throw new PluginError("bad-request", "persist/append requires events");
		try {
			await record.handle.append(events);
		} catch (error) {
			mapPersistenceError(error);
		}
		return {};
	}
	async flush(params) {
		if (params["handleId"] === void 0) {
			try {
				await this.persistence().flush();
			} catch (error) {
				mapPersistenceError(error);
			}
			return {};
		}
		const record = this.require(String(params["handleId"]));
		try {
			await record.handle.flush();
		} catch (error) {
			mapPersistenceError(error);
		}
		return {};
	}
	async close(params) {
		const handleId = typeof params["handleId"] === "string" ? params["handleId"] : "";
		const record = this.require(handleId);
		this.handles.delete(handleId);
		try {
			await record.handle.close();
		} catch (error) {
			mapPersistenceError(error);
		}
		return {};
	}
	/** Close every handle held by this process (plugin unload). */
	async dispose() {
		const records = [...this.handles.values()];
		this.handles.clear();
		await Promise.allSettled(records.map((record) => record.handle.close()));
	}
	/** Executor write-open guard table (protocol §16.5). */
	async guardWriteOpen(sessionId) {
		const record = await this.executor.get(sessionId);
		const owner = this.ownerOf();
		if (record === null) {
			if (this.ctx.get("agents")?.get(sessionId) !== void 0) modeConflict("server", { liveAgent: true });
			await this.executor.assign(sessionId, "local", owner, "handoff");
			return;
		}
		if (record.mode === "server") modeConflict("server");
		const claim = record.owner;
		if (claim === void 0) throw new PluginError("executor-held", "the local executor has no owner; run executor/assign to take it over", { sessionId });
		if (owner !== void 0 && claim.clientId === owner.clientId && claim.instanceId === owner.instanceId) return;
		const age = Date.now() - Date.parse(claim.heartbeatAt);
		if (!Number.isFinite(age) || age > OWNER_TTL_MS) throw new PluginError("executor-held", "the previous local executor lease expired; run executor/assign to take it over", {
			ownerClientId: claim.clientId,
			heartbeatAt: claim.heartbeatAt
		});
		throw new PluginError("executor-held", "this session is being written by another local executor", {
			ownerClientId: claim.clientId,
			heartbeatAt: claim.heartbeatAt
		});
	}
};
//#endregion
//#region src/server-plugin/session-rename.ts
/** Append one durable user `session/title`; no agent is created or resumed. */
async function renameStoredSession(ctx, params) {
	const sessionId = typeof params["sessionId"] === "string" ? params["sessionId"] : "";
	const title = (typeof params["title"] === "string" ? params["title"] : "").trim();
	if (sessionId === "") throw new PluginError("bad-request", "session/rename requires sessionId");
	if (title === "") throw new PluginError("bad-request", "session/rename requires a non-empty title");
	const persistence = ctx.get("sessionPersistence");
	if (persistence === void 0) throw new PluginError("internal", "the profile has no sessionPersistence service");
	let handle;
	try {
		handle = await persistence.open(sessionId, "write");
	} catch (error) {
		mapPersistenceError(error);
	}
	try {
		const { events } = await handle.read(0);
		const seq = events.length;
		await handle.append([{
			type: "session/title",
			seq,
			time: Date.now(),
			data: {
				title,
				messageSeqs: [],
				source: { kind: "user" }
			}
		}]);
		await handle.flush();
		return {
			title,
			seq
		};
	} catch (error) {
		mapPersistenceError(error);
	} finally {
		await handle.close().catch(() => {});
	}
}
//#endregion
//#region src/server-plugin/index.ts
/**
* dsh-remote-workspace-server v1 profile plugin (Phase A skeleton).
*
* Runs inside a per-client profile's `dsh web` process and exposes the P1
* client-host API (handshake/lease/RPC/SSE/mux) plus the embed injection.
* All official services are consumed through `ctx.get()` so this package keeps
* a single external boundary: the DSH runtime that installed it.
*/
/** Cordis row name. */
const name = "dsh-remote-workspace-server";
/** The web carrier is the only required service; everything else is optional and probed. */
const inject = [
	"webServer",
	"connection",
	"fs",
	"subprocess",
	"sandbox",
	"sessionPersistence"
];
/** The pinned DSH runtime this package was verified against. */
const DSH_VERIFIED_RUNTIME = "0.2.1-rc.3";
/** Read the package version from the installed package root. */
function readPluginVersion() {
	try {
		const url = new URL("../package.json", import.meta.url);
		const value = JSON.parse(readFileSync(url, "utf8"));
		return typeof value.version === "string" ? value.version : REMOTE_WORKSPACE_PLUGIN_VERSION;
	} catch {
		return REMOTE_WORKSPACE_PLUGIN_VERSION;
	}
}
/** SHA-256 revision of the workspace registry file, or `absent`. */
async function registryRevision(file) {
	try {
		return createHash("sha256").update(await readFile(file)).digest("hex");
	} catch (error) {
		if (error.code === "ENOENT") return "absent";
		throw error;
	}
}
/** Read and validate a JSON body under the 1 MiB contract. */
async function readJson(req, limit = 1048576) {
	const chunks = [];
	let total = 0;
	for await (const chunk of req) {
		const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
		total += bytes.length;
		if (total > limit) throw new PluginError("bad-request", "request body exceeds 1 MiB");
		chunks.push(bytes);
	}
	if (total === 0) return {};
	try {
		return JSON.parse(Buffer.concat(chunks).toString("utf8"));
	} catch {
		throw new PluginError("bad-request", "request body is not JSON");
	}
}
/** Send one JSON envelope. */
function sendJson(res, status, value) {
	const body = JSON.stringify(value);
	res.writeHead(status, {
		"content-type": "application/json; charset=utf-8",
		"cache-control": "no-store"
	});
	res.end(body);
}
function sendOk(res, value) {
	sendJson(res, 200, {
		ok: true,
		value
	});
}
function sendError(res, error) {
	const value = errorValue(error);
	sendJson(res, httpStatusFor(value.code), {
		ok: false,
		error: value
	});
}
function header(req, name) {
	const value = req.headers[name];
	return Array.isArray(value) ? value[0] : value;
}
/** Resolve all paths from config/profileContext. */
function resolvePaths(ctx, config) {
	const profileContext = ctx.get("profileContext");
	const profile = config.profile ?? profileContext?.name;
	if (profile === void 0 || profile === "") return void 0;
	const home = config.home ?? profileContext?.home ?? process.env["DSH_HOME"] ?? join(process.env["HOME"] ?? process.cwd(), ".dsh");
	const profileDir = config.profileDir ?? profileContext?.dir ?? join(home, "profiles", profile);
	const suffix = profile.startsWith("graydovee-remote-workspace-") ? profile.slice(27) : profile;
	return {
		profile,
		clientId: config.clientId ?? suffix,
		profileDir,
		home,
		registryFile: config.registryFile ?? join(home, "storages", "workspace.json"),
		leaseFile: join(profileDir, ".dsh-rw", "lease.json"),
		lockDir: join(home, "remote-workspace", "locks")
	};
}
function capabilities(state) {
	const guard = state.registerGuard?.();
	return {
		workspaceApi: 1,
		sessionApi: 1,
		persistRpc: 1,
		executor: 1,
		eventsSse: 1,
		muxWs: 1,
		...state.clientTargets === void 0 ? {} : { clientTargets: 1 },
		embed: {
			storagePrefix: `dshrw:${state.paths.profile}:`,
			mode: "reload"
		},
		registryWriteGuard: guard?.capability ?? "none",
		registryReload: guard?.reload ?? "none"
	};
}
function activeAgents(ctx) {
	const agents = ctx.get("agents");
	if (agents === void 0) return [];
	return agents.list().map((agent) => ({
		sessionId: String(agent.id),
		running: agent.status === "running",
		status: agent.status
	}));
}
function workspaceRow(workspace) {
	return {
		workspaceId: String(workspace.id),
		path: workspace.path,
		title: workspace.title,
		createdAt: workspace.createdAt,
		updatedAt: workspace.updatedAt,
		sessionIds: [...workspace.sessionIds]
	};
}
async function workspaceList(ctx, state) {
	const registry = ctx.get("workspaceRegistry");
	if (registry === void 0) throw new PluginError("registry-unavailable", "the profile has no workspaceRegistry service");
	await state.guardProbe?.();
	const guard = state.registerGuard?.();
	return {
		registry: {
			revision: guard?.revision ?? await registryRevision(state.paths.registryFile),
			epoch: guard?.epoch ?? "0",
			initialized: registry.list().length > 0
		},
		workspaces: registry.list().map(workspaceRow),
		pinnedSessionIds: [...registry.pinnedSessionIds],
		archivedSessionIds: [...registry.archivedSessionIds]
	};
}
/** Cancel every live agent, wait for turn tails, then flush the official log. */
async function cancelAll(ctx) {
	const agents = ctx.get("agents");
	const controller = ctx.get("sessionController");
	const persistence = ctx.get("sessionPersistence");
	const canceled = [];
	const idle = [];
	for (const agent of agents?.list() ?? []) try {
		controller?.cancel({ sessionId: String(agent.id) });
		canceled.push(String(agent.id));
		idle.push(agent.whenIdle());
	} catch (error) {
		console.error(`[dsh-remote-workspace-server] cancel ${String(agent.id)} failed: ${scrubSecrets(error instanceof Error ? error.message : String(error))}`);
	}
	const settled = await Promise.race([Promise.allSettled(idle), new Promise((resolve) => {
		setTimeout(() => {
			resolve("timeout");
		}, 3e3).unref();
	})]);
	const flush = persistence?.flush();
	const flushed = await Promise.race([Promise.resolve(flush).then(() => true, () => false), new Promise((resolve) => {
		setTimeout(() => {
			resolve(false);
		}, 3e3).unref();
	})]);
	if (settled === "timeout" || flushed === false) throw new PluginError("shutdown-flush-timeout", "turn tail or session log flush did not settle within 3 s", {
		sessions: canceled,
		waitedMs: 3e3
	});
	return { canceled };
}
/** Register the handshake/lease/RPC/SSE/mux routes and embed injection. */
function installRoutes(ctx, config, state, lease) {
	const webServer = ctx.get("webServer");
	if (webServer === void 0) throw new Error("dsh-remote-workspace-server requires ctx.webServer");
	const admit = (req) => {
		const connection = ctx.get("connection");
		if (connection === void 0) return false;
		return !("rejection" in connection.admit(req));
	};
	ctx.effect(() => webServer.register({
		kind: "exact",
		path: RW_PATHS.handshake,
		handler: async (req, res) => {
			if (!admit(req)) {
				sendJson(res, 401, {
					ok: false,
					error: {
						code: "unauthorized",
						message: "authentication required"
					}
				});
				return;
			}
			try {
				sendOk(res, await handshakeResponse(ctx, state, lease, await readJson(req)));
			} catch (error) {
				sendError(res, error);
			}
		}
	}), "dsh-rw: handshake route");
	ctx.effect(() => webServer.register({
		kind: "exact",
		path: RW_PATHS.rpc,
		handler: async (req, res) => {
			if (!admit(req)) {
				sendJson(res, 401, {
					ok: false,
					error: {
						code: "unauthorized",
						message: "authentication required"
					}
				});
				return;
			}
			try {
				if (!lease.matches(header(req, RW_HEADERS.client), header(req, RW_HEADERS.instance))) throw new PluginError("lease-mismatch", "the request headers do not match the active profile lease");
				const body = await readJson(req);
				if (body === null || typeof body !== "object" || typeof body.method !== "string") throw new PluginError("bad-request", "expected { protocol, method, params }");
				if (Number(body.protocol) !== 1) throw new PluginError("plugin-protocol-mismatch", `unsupported plugin protocol ${String(body.protocol)}; this server speaks 1`, { serverProtocol: 1 });
				sendOk(res, await dispatch(ctx, state, lease, body.method, body.params));
			} catch (error) {
				sendError(res, error);
			}
		}
	}), "dsh-rw: rpc route");
	ctx.effect(() => webServer.register({
		kind: "exact",
		path: RW_PATHS.bootstrap,
		handler: (req, res) => {
			res.writeHead(200, {
				"content-type": "text/javascript; charset=utf-8",
				"cache-control": "no-store"
			});
			res.end(BOOTSTRAP_SCRIPT);
		}
	}), "dsh-rw: bootstrap route");
	ctx.effect(() => webServer.register({
		kind: "exact",
		path: RW_PATHS.embedCss,
		handler: (req, res) => {
			res.writeHead(200, {
				"content-type": "text/css; charset=utf-8",
				"cache-control": "no-store"
			});
			res.end(EMBED_CSS);
		}
	}), "dsh-rw: embed css route");
	ctx.effect(() => webServer.register({
		kind: "exact",
		path: RW_PATHS.clientTargets,
		handler: (req, res) => {
			if (!admit(req)) {
				sendJson(res, 401, {
					ok: false,
					error: {
						code: "unauthorized",
						message: "authentication required"
					}
				});
				return;
			}
			sendOk(res, state.clientTargets?.read() ?? emptyClientTargets());
		}
	}), "dsh-rw: client targets route");
	ctx.effect(() => webServer.register({
		kind: "exact",
		path: RW_PATHS.events,
		handler: async (req, res) => {
			if (!admit(req)) {
				res.writeHead(401);
				res.end("unauthorized");
				return;
			}
			res.writeHead(200, {
				"content-type": "text/event-stream; charset=utf-8",
				"cache-control": "no-store",
				"x-accel-buffering": "no"
			});
			const hello = {
				protocol: 1,
				profile: state.paths.profile,
				instance: lease.current?.instanceId,
				registry: {
					revision: state.registerGuard?.()?.revision ?? await registryRevision(state.paths.registryFile),
					epoch: state.registerGuard?.()?.epoch ?? "0",
					initialized: true
				},
				capabilities: capabilities(state)
			};
			res.write(`event: hello\ndata: ${JSON.stringify(hello)}\n\n`);
			const ping = setInterval(() => {
				res.write(": ping\n\n");
			}, 15e3);
			ping.unref();
			req.on("close", () => clearInterval(ping));
		}
	}), "dsh-rw: events route");
	ctx.effect(() => webServer.register({
		kind: "exact",
		path: RW_PATHS.sessionEvents,
		handler: async (req, res) => {
			if (!admit(req)) {
				res.writeHead(401);
				res.end("unauthorized");
				return;
			}
			try {
				const client = header(req, RW_HEADERS.client);
				const instance = header(req, RW_HEADERS.instance);
				if ((client !== void 0 || instance !== void 0) && !lease.matches(client, instance)) throw new PluginError("lease-mismatch", "the session-events headers do not match the active profile lease");
				const params = sessionEventParams(req.url);
				res.writeHead(200, {
					"content-type": "text/event-stream; charset=utf-8",
					"cache-control": "no-store",
					"x-accel-buffering": "no"
				});
				res.flushHeaders?.();
				const abort = new AbortController();
				req.on("close", () => abort.abort());
				const sink = {
					send: (frame) => {
						if (!res.writableEnded) res.write(`event: ${frame.event}\ndata: ${JSON.stringify(frame.data)}\n\n`);
					},
					comment: (text) => {
						if (!res.writableEnded) res.write(`: ${text}\n\n`);
					},
					closed: () => res.writableEnded || abort.signal.aborted
				};
				try {
					await streamSessionEvents(ctx, {
						sessionId: params.sessionId,
						afterSeq: params.afterSeq,
						sink,
						signal: abort.signal,
						...state.approvals === void 0 ? {} : { approvals: state.approvals }
					});
				} catch (error) {
					const value = errorValue(error);
					if (!res.writableEnded) res.write(`event: error\ndata: ${JSON.stringify(value)}\n\n`);
				}
				if (!res.writableEnded) res.end();
			} catch (error) {
				sendError(res, error);
			}
		}
	}), "dsh-rw: session events route");
	ctx.effect(() => webServer.registerUpgrade({
		path: RW_PATHS.mux,
		handler: (req, socket) => {
			if (!admit(req) || !lease.matches(header(req, RW_HEADERS.client), header(req, RW_HEADERS.instance))) {
				socket.write("HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n");
				socket.destroy();
				return;
			}
			serveMux(ctx, lease, req, socket, Buffer.alloc(0));
		}
	}), "dsh-rw: mux upgrade");
	ctx.effect(() => webServer.tapIndex((html) => {
		if (html.includes("__DSH_RW_EMBED__")) return html;
		const embed = {
			protocol: 1,
			storagePrefix: `dshrw:${state.paths.profile}:`,
			embedCssPath: RW_PATHS.embedCss.slice(1)
		};
		const injection = `<script>window.__DSH_RW_EMBED__=${JSON.stringify(embed)}<\/script><script src="${RW_PATHS.bootstrap.slice(1)}"><\/script>`;
		return html.replace("<head>", `<head>${injection}`);
	}), "dsh-rw: embed index injection");
}
/** Validate handshake fields before claiming the lease. */
function validateHandshake(state, body) {
	if (Number(body["protocol"]) !== 1) throw new PluginError("plugin-protocol-mismatch", `unsupported plugin protocol ${String(body["protocol"])}; this server speaks 1`, {
		clientProtocol: body["protocol"],
		serverProtocol: 1
	});
	const clientId = typeof body["clientId"] === "string" ? body["clientId"] : "";
	const instanceId = typeof body["instanceId"] === "string" ? body["instanceId"] : "";
	const profile = typeof body["profile"] === "string" ? body["profile"] : "";
	const pluginVersion = typeof body["pluginVersion"] === "string" ? body["pluginVersion"] : "";
	if (clientId === "" || instanceId === "" || profile === "") throw new PluginError("bad-request", "handshake requires clientId, instanceId and profile");
	if (profile !== state.paths.profile) throw new PluginError("profile-mismatch", `this profile is "${state.paths.profile}", not "${profile}"`);
	if (clientId !== state.paths.clientId) throw new PluginError("profile-foreign-client", `profile "${state.paths.profile}" belongs to client "${state.paths.clientId}"`);
	if (pluginVersion !== state.pluginVersion) throw new PluginError("plugin-version-mismatch", `server plugin ${state.pluginVersion} does not match client plugin ${pluginVersion}; reinstall the server plugin`, {
		clientVersion: pluginVersion,
		serverVersion: state.pluginVersion
	});
}
async function handshakeResponse(ctx, state, lease, body) {
	validateHandshake(state, body);
	const token = typeof body["tokenFingerprint"] === "string" ? body["tokenFingerprint"] : "";
	const decision = await lease.claim({
		clientId: String(body["clientId"]),
		instanceId: String(body["instanceId"]),
		port: ctx.get("webServer")?.port ?? 0,
		tokenFingerprint: createHash("sha256").update(token).digest("hex").slice(0, 16)
	});
	if (decision.kind === "foreign-client") throw new PluginError("profile-foreign-client", `this profile belongs to client "${decision.ownerClientId}"`);
	if (decision.kind === "in-use") throw new PluginError("profile-in-use", "this profile is being driven by another client instance", {
		ownerInstanceId: decision.owner.instanceId,
		ownerPid: decision.owner.pid,
		startedAt: decision.owner.startedAt
	});
	await Promise.race([state.guardReady, new Promise((resolve) => {
		setTimeout(resolve, 1e3).unref();
	})]);
	await state.guardProbe?.();
	const guard = state.registerGuard?.();
	return {
		protocol: 1,
		pluginVersion: state.pluginVersion,
		dshVersion: DSH_VERIFIED_RUNTIME,
		profile: state.paths.profile,
		clientId: state.paths.clientId,
		lease: {
			instanceId: decision.lease.instanceId,
			expiresAt: new Date(Date.parse(decision.lease.heartbeatAt) + 2e4).toISOString()
		},
		staleTakeover: decision.staleTakeover,
		registry: {
			initialized: true,
			revision: guard?.revision ?? await registryRevision(state.paths.registryFile),
			epoch: guard?.epoch ?? "0"
		},
		capabilities: capabilities(state),
		activeAgents: activeAgents(ctx)
	};
}
function workspaceRegistry(ctx) {
	const registry = ctx.get("workspaceRegistry");
	if (registry === void 0) throw new PluginError("registry-unavailable", "the profile has no workspaceRegistry service");
	return registry;
}
function workspaceEntity(ctx, workspaceId) {
	const found = workspaceRegistry(ctx).list().find((workspace) => String(workspace.id) === workspaceId);
	if (found === void 0) throw new PluginError("workspace-unknown", `unknown workspace "${workspaceId}"`, { workspaceId });
	return found;
}
async function workspaceCreate(ctx, params) {
	const path = typeof params["path"] === "string" ? params["path"] : "";
	if (!path.startsWith("/")) throw new PluginError("workspace-path-invalid", "workspace path must be an absolute server path", { path });
	try {
		return workspaceRow(await workspaceRegistry(ctx).create(path, typeof params["title"] === "string" ? params["title"] : void 0));
	} catch (error) {
		const code = error.code;
		if (code === "ENOENT" || code === "ENOTDIR") throw new PluginError("workspace-path-invalid", `workspace path is not an existing directory: ${path}`, { path });
		throw error;
	}
}
async function workspaceRename(ctx, params) {
	const workspaceId = typeof params["workspaceId"] === "string" ? params["workspaceId"] : "";
	const title = typeof params["title"] === "string" ? params["title"].trim() : "";
	if (title === "") throw new PluginError("bad-request", "workspace title must not be empty");
	await workspaceEntity(ctx, workspaceId).setTitle(title);
	return workspaceRow(workspaceEntity(ctx, workspaceId));
}
async function workspaceDelete(ctx, params) {
	const workspaceId = typeof params["workspaceId"] === "string" ? params["workspaceId"] : "";
	const workspace = workspaceEntity(ctx, workspaceId);
	if (params["force"] !== true && workspace.sessionIds.length > 0) throw new PluginError("workspace-not-empty", `workspace "${workspaceId}" still has ${workspace.sessionIds.length} sessions`, {
		sessionCount: workspace.sessionIds.length,
		sessionIds: workspace.sessionIds
	});
	return { deleted: await workspaceRegistry(ctx).delete(workspaceId) };
}
async function workspaceReorder(ctx, params) {
	const workspaceId = typeof params["workspaceId"] === "string" ? params["workspaceId"] : "";
	const before = typeof params["beforeWorkspaceId"] === "string" ? params["beforeWorkspaceId"] : void 0;
	return { order: await workspaceRegistry(ctx).insertBefore(workspaceId, before) };
}
function mapArchiveError(error) {
	if (error?.name === "WorkspaceActiveSessionError") throw new PluginError("session/archive-activity", error.message, { activity: error.activity });
	throw error;
}
async function sessionList(ctx, state, params) {
	await state.guardProbe?.();
	const registry = workspaceRegistry(ctx);
	const workspaces = registry.list();
	const workspaceId = typeof params["workspaceId"] === "string" ? params["workspaceId"] : void 0;
	const pinned = new Set(registry.pinnedSessionIds.map(String));
	const archived = new Set(registry.archivedSessionIds.map(String));
	const workspaceOf = /* @__PURE__ */ new Map();
	for (const workspace of workspaces) for (const id of workspace.sessionIds) workspaceOf.set(String(id), String(workspace.id));
	const controller = ctx.get("sessionController");
	const persistence = ctx.get("sessionPersistence");
	const summaries = [];
	if (controller?.list !== void 0) try {
		summaries.push(...(await controller.list({}, AbortSignal.timeout(1e4))).items);
	} catch (error) {
		console.error(`[dsh-remote-workspace-server] sessionController.list failed, falling back to persistence: ${error instanceof Error ? error.message : String(error)}`);
	}
	if (summaries.length === 0 && persistence?.list !== void 0) for (const snapshot of await persistence.list()) {
		const header = snapshot.header;
		if (typeof header.id !== "string") continue;
		summaries.push({
			sessionId: header.id,
			cwd: typeof header.cwd === "string" ? header.cwd : void 0
		});
	}
	const roleOrder = workspaceId === void 0 ? void 0 : workspaces.find((workspace) => String(workspace.id) === workspaceId)?.sessionIds.map(String);
	const sessions = [];
	for (const summary of summaries) {
		const id = String(summary.sessionId);
		if (workspaceId !== void 0 && workspaceOf.get(id) !== workspaceId) continue;
		const stored = await persistence?.stat?.(id).catch(() => void 0);
		const sizeBytes = stored?.sizeBytes;
		const projectionTitle = summary.projections?.values?.title;
		const liveTitle = summary.agentAvailable === true ? summary.title : void 0;
		let title = liveTitle ?? (typeof projectionTitle === "string" && projectionTitle !== "" ? projectionTitle : void 0);
		let titleSource = liveTitle !== void 0 ? "live" : title !== void 0 ? "projection" : "fallback";
		const metadataBlank = summary.projections?.values?.sessionListMetadata?.blank;
		const live = summary.agentAvailable === true;
		let blank = resolveBlank(metadataBlank, live, summary.blank, void 0);
		let updatedAt = summary.updatedAt ?? 0;
		if (title === void 0 || updatedAt <= 0 || metadataBlank === void 0 && !live) {
			const folded = await state.titleFold.facts(id, sizeBytes);
			if (title === void 0 && folded?.title !== void 0) {
				title = folded.title;
				titleSource = "log";
			}
			blank = resolveBlank(metadataBlank, live, summary.blank, folded?.blank);
			if (updatedAt <= 0 && folded?.updatedAt !== void 0) updatedAt = folded.updatedAt;
		}
		const headerCreatedAt = typeof stored?.header?.["createdAt"] === "number" ? stored.header["createdAt"] : 0;
		if (updatedAt <= 0) updatedAt = headerCreatedAt;
		sessions.push({
			sessionId: id,
			executor: executorModeOf(await state.executor?.get(id)),
			...title === void 0 ? {} : {
				title,
				titleSource
			},
			blank,
			running: summary.running === true,
			agentAvailable: summary.agentAvailable === true,
			...summary.cwd === void 0 ? {} : { cwd: summary.cwd },
			updatedAt,
			...summary.parentSessionId === void 0 ? {} : { parentSessionId: summary.parentSessionId },
			...summary.origin === void 0 ? {} : { origin: summary.origin },
			...summary.formatStatus === void 0 ? {} : { formatStatus: summary.formatStatus },
			pinned: pinned.has(id),
			archived: archived.has(id),
			...workspaceOf.get(id) === void 0 ? {} : { workspaceId: workspaceOf.get(id) }
		});
	}
	if (roleOrder !== void 0) {
		const rank = new Map(roleOrder.map((id, index) => [id, index]));
		sessions.sort((left, right) => (rank.get(left.sessionId) ?? Number.MAX_SAFE_INTEGER) - (rank.get(right.sessionId) ?? Number.MAX_SAFE_INTEGER));
	} else sessions.sort((left, right) => right.updatedAt - left.updatedAt);
	const guard = state.registerGuard?.();
	return {
		registry: {
			revision: guard?.revision ?? await registryRevision(state.paths.registryFile),
			epoch: guard?.epoch ?? "0"
		},
		sessions
	};
}
async function persistList(ctx) {
	const persistence = ctx.get("sessionPersistence");
	if (persistence?.list === void 0) throw new PluginError("internal", "sessionPersistence.list is unavailable");
	return { snapshots: (await persistence.list()).map((snapshot) => ({
		header: snapshot.header,
		...snapshot.sizeBytes === void 0 ? {} : { sizeBytes: snapshot.sizeBytes },
		...snapshot.eventCount === void 0 ? {} : { eventCount: snapshot.eventCount },
		...snapshot.formatStatus === void 0 ? {} : { formatStatus: snapshot.formatStatus },
		...snapshot.revision === void 0 ? {} : { revision: String(snapshot.revision) }
	})) };
}
async function persistLocate(ctx, params) {
	const sessionId = typeof params["sessionId"] === "string" ? params["sessionId"] : "";
	const snapshot = await ctx.get("sessionPersistence")?.stat?.(sessionId);
	if (snapshot === void 0) return null;
	const cwd = snapshot.header["cwd"];
	if (typeof cwd !== "string") return null;
	const workspace = workspaceRegistry(ctx).list().find((candidate) => candidate.path === cwd);
	return {
		remoteCwd: cwd,
		...workspace === void 0 ? {} : { workspaceId: String(workspace.id) }
	};
}
function leaseOwner(lease) {
	const current = lease.current;
	return current === void 0 ? void 0 : {
		clientId: current.clientId,
		instanceId: current.instanceId,
		heartbeatAt: current.heartbeatAt
	};
}
function asRecord(value) {
	return value !== null && typeof value === "object" && !Array.isArray(value) ? value : {};
}
async function dispatch(ctx, state, lease, method, params) {
	switch (method) {
		case RW_METHODS.heartbeat: {
			const value = await lease.heartbeat();
			const guard = state.registerGuard?.();
			return {
				expiresAt: value.expiresAt,
				registry: {
					revision: guard?.revision ?? await registryRevision(state.paths.registryFile),
					stale: guard?.stale ?? false,
					epoch: guard?.epoch ?? "0"
				}
			};
		}
		case RW_METHODS.release:
			await lease.release();
			state.clientTargets?.clear();
			return {};
		case RW_METHODS.status: {
			const guard = state.registerGuard?.();
			return {
				lease: lease.current,
				registry: {
					revision: guard?.revision ?? await registryRevision(state.paths.registryFile),
					loadedRevision: guard?.loadedRevision ?? await registryRevision(state.paths.registryFile),
					stale: guard?.stale ?? false,
					epoch: guard?.epoch ?? "0"
				},
				activeAgents: activeAgents(ctx)
			};
		}
		case RW_METHODS.cancelAll: return await cancelAll(ctx);
		case RW_METHODS.workspaceList: return await workspaceList(ctx, state);
		case RW_METHODS.registryStatus: {
			await state.guardProbe?.();
			const guard = state.registerGuard?.();
			return {
				revision: guard?.revision ?? await registryRevision(state.paths.registryFile),
				loadedRevision: guard?.loadedRevision ?? await registryRevision(state.paths.registryFile),
				stale: guard?.stale ?? false,
				epoch: guard?.epoch ?? "0"
			};
		}
		case RW_METHODS.registryRefresh:
			if (state.refreshRegistry === void 0) throw new PluginError("registry-reload-failed", "in-process registry reload is unavailable in this build");
			return await state.refreshRegistry();
		case RW_METHODS.workspaceCreate: return await workspaceCreate(ctx, asRecord(params));
		case RW_METHODS.workspaceRename: return await workspaceRename(ctx, asRecord(params));
		case RW_METHODS.workspaceDelete: return await workspaceDelete(ctx, asRecord(params));
		case RW_METHODS.workspaceReorder: return await workspaceReorder(ctx, asRecord(params));
		case RW_METHODS.sessionList: return await sessionList(ctx, state, asRecord(params));
		case RW_METHODS.sessionPrepareServer: {
			const sessionId = asRecord(params)["sessionId"];
			if (typeof sessionId !== "string") throw new PluginError("bad-request", "session/prepare-server requires sessionId");
			if (state.managed === void 0) throw new PluginError("agent-compose-failed", "managed agents are unavailable in this build");
			return await state.managed.prepare(sessionId);
		}
		case RW_METHODS.sessionCreate: {
			const input = asRecord(params);
			const executor = input["executor"] === "local" ? "local" : "server";
			const workspaceId = typeof input["workspaceId"] === "string" ? input["workspaceId"] : void 0;
			const workspace = workspaceId === void 0 ? void 0 : workspaceRegistry(ctx).list().find((candidate) => String(candidate.id) === workspaceId);
			if (workspaceId !== void 0 && workspace === void 0) throw new PluginError("workspace-unknown", `unknown workspace "${workspaceId}"`, { workspaceId });
			const cwd = workspace?.path ?? (typeof input["cwd"] === "string" ? input["cwd"] : void 0);
			if (cwd === void 0 || !cwd.startsWith("/")) throw new PluginError("bad-request", "session/create requires workspaceId or an absolute cwd");
			const minted = typeof input["sessionId"] === "string" && input["sessionId"] !== "" ? input["sessionId"] : `session-${randomUUID()}`;
			if (executor === "local") {
				if (workspace === void 0) throw new PluginError("bad-request", "local session/create requires workspaceId");
				if (state.executor === void 0) throw new PluginError("internal", "executor store is unavailable");
				await state.executor.assign(minted, "local", leaseOwner(lease), "handoff");
				return {
					sessionId: minted,
					executor: "local"
				};
			}
			if (state.managed === void 0) throw new PluginError("agent-compose-failed", "managed agents are unavailable in this build");
			const created = await state.managed.createServerBlank(minted, cwd, typeof input["agentPreset"] === "string" ? input["agentPreset"] : void 0);
			try {
				if (workspace !== void 0) await workspace.attachSession(minted);
			} catch (error) {
				await state.managed.dispose(minted);
				throw new PluginError("internal", `blank session was created but could not attach to workspace "${String(workspace?.id)}": ${error instanceof Error ? error.message : String(error)}`);
			}
			if (state.executor !== void 0) await state.executor.assign(minted, "server", void 0, "default");
			return {
				sessionId: minted,
				executor: "server",
				...created
			};
		}
		case RW_METHODS.sessionRename: {
			const controller = ctx.get("sessionController");
			if (controller?.rename !== void 0) return await controller.rename(asRecord(params));
			return await renameStoredSession(ctx, asRecord(params));
		}
		case RW_METHODS.sessionFork: {
			const controller = ctx.get("sessionController");
			if (controller?.fork === void 0) throw new PluginError("internal", "sessionController.fork is unavailable");
			return await controller.fork(asRecord(params));
		}
		case RW_METHODS.sessionCancel: {
			const controller = ctx.get("sessionController");
			const input = asRecord(params);
			const sessionId = input["sessionId"];
			if (typeof sessionId !== "string") throw new PluginError("bad-request", "session/cancel requires sessionId");
			if (controller?.cancel !== void 0) {
				controller.cancel({ sessionId });
				return { canceled: true };
			}
			return await cancelSession(ctx, state.managed, input);
		}
		case RW_METHODS.sessionPin:
		case RW_METHODS.sessionArchive: {
			const input = asRecord(params);
			const sessionId = typeof input["sessionId"] === "string" ? input["sessionId"] : "";
			const registry = workspaceRegistry(ctx);
			if (method === RW_METHODS.sessionPin) {
				try {
					if (input["pinned"] === true) await registry.pinSession(sessionId);
					else await registry.unpinSession(sessionId);
				} catch (error) {
					if (error?.name === "WorkspaceArchivedSessionPinError") throw new PluginError("session/archived-pin", error.message, { sessionId });
					if (error?.name === "WorkspaceUnknownSessionError") throw new PluginError("session-not-found", error.message, { sessionId });
					throw error;
				}
				return { pinned: [...registry.pinnedSessionIds] };
			}
			try {
				if (input["archived"] === true) await registry.archiveSession(sessionId, { stopActivity: input["stopActivity"] === true });
				else await registry.unarchiveSession(sessionId);
			} catch (error) {
				mapArchiveError(error);
			}
			return { archived: [...registry.archivedSessionIds] };
		}
		case RW_METHODS.sessionReorder: {
			const input = asRecord(params);
			const workspace = workspaceEntity(ctx, typeof input["workspaceId"] === "string" ? input["workspaceId"] : "");
			const sessionId = typeof input["sessionId"] === "string" ? input["sessionId"] : "";
			const before = typeof input["beforeSessionId"] === "string" ? input["beforeSessionId"] : void 0;
			await workspace.insertSessionBefore(sessionId, before);
			return { order: [...workspaceEntity(ctx, typeof input["workspaceId"] === "string" ? input["workspaceId"] : "").sessionIds] };
		}
		case RW_METHODS.persistList: return await persistList(ctx);
		case RW_METHODS.persistStat: {
			const persistence = ctx.get("sessionPersistence");
			const sessionId = asRecord(params)["sessionId"];
			if (typeof sessionId !== "string") throw new PluginError("bad-request", "persist/stat requires sessionId");
			const snapshot = await persistence?.stat?.(sessionId);
			return snapshot === void 0 ? null : {
				header: snapshot.header,
				...snapshot.sizeBytes === void 0 ? {} : { sizeBytes: snapshot.sizeBytes },
				...snapshot.eventCount === void 0 ? {} : { eventCount: snapshot.eventCount }
			};
		}
		case RW_METHODS.persistLocate: return await persistLocate(ctx, asRecord(params));
		case RW_METHODS.fsList: return await listDirectory(ctx, asRecord(params));
		case RW_METHODS.sessionTimeline: return await readTimeline(ctx, asRecord(params));
		case RW_METHODS.sessionPrompt: return await promptSession(ctx, state.managed, asRecord(params));
		case RW_METHODS.sessionCommands: return await listSessionCommands(ctx, state.managed, asRecord(params));
		case RW_METHODS.sessionCommand: return await executeSessionCommand(ctx, state.managed, asRecord(params));
		case RW_METHODS.sessionModelCatalog: {
			const controller = state.sessionController ?? ctx.get("sessionController");
			if (controller?.modelCatalog === void 0) throw new PluginError("agent-unavailable", "the profile has no sessionController model catalog");
			return await controller.modelCatalog();
		}
		case RW_METHODS.sessionSelectModel: {
			const body = asRecord(params);
			const sessionId = typeof body["sessionId"] === "string" ? body["sessionId"] : "";
			const provider = typeof body["provider"] === "string" ? body["provider"] : "";
			const model = typeof body["model"] === "string" ? body["model"] : "";
			if (sessionId === "" || provider === "" || model === "") throw new PluginError("bad-request", "session/selectModel requires sessionId, provider and model");
			const controller = state.sessionController ?? ctx.get("sessionController");
			if (controller?.selectModel === void 0) throw new PluginError("agent-unavailable", "the profile has no sessionController model selector");
			return await controller.selectModel({
				sessionId,
				provider,
				model,
				...body["reasoningEffort"] === void 0 ? {} : { reasoningEffort: body["reasoningEffort"] }
			});
		}
		case RW_METHODS.sessionApproval:
			if (state.approvals === void 0) throw new PluginError("internal", "the approval hub is unavailable");
			return approveSession(state.approvals, asRecord(params));
		case RW_METHODS.sessionStatus:
			if (state.approvals === void 0) throw new PluginError("internal", "the approval hub is unavailable");
			return await sessionStatusValue(ctx, state.approvals, asRecord(params), state.approvalGate);
		case RW_METHODS.persistCreate:
			if (state.persist === void 0) throw new PluginError("internal", "persist bridge is unavailable");
			return await state.persist.create(asRecord(params));
		case RW_METHODS.persistOpen:
			if (state.persist === void 0) throw new PluginError("internal", "persist bridge is unavailable");
			return await state.persist.open(asRecord(params));
		case RW_METHODS.persistRead:
			if (state.persist === void 0) throw new PluginError("internal", "persist bridge is unavailable");
			return await state.persist.read(asRecord(params));
		case RW_METHODS.persistAppend:
			if (state.persist === void 0) throw new PluginError("internal", "persist bridge is unavailable");
			return await state.persist.append(asRecord(params));
		case RW_METHODS.persistFlush:
			if (state.persist === void 0) throw new PluginError("internal", "persist bridge is unavailable");
			return await state.persist.flush(asRecord(params));
		case RW_METHODS.persistClose:
			if (state.persist === void 0) throw new PluginError("internal", "persist bridge is unavailable");
			return await state.persist.close(asRecord(params));
		case RW_METHODS.executorGet: {
			const sessionId = asRecord(params)["sessionId"];
			if (typeof sessionId !== "string" || state.executor === void 0) return null;
			return await state.executor.get(sessionId);
		}
		case RW_METHODS.executorAssign: {
			const input = asRecord(params);
			const sessionId = typeof input["sessionId"] === "string" ? input["sessionId"] : "";
			const mode = input["mode"] === "local" || input["mode"] === "server" ? input["mode"] : void 0;
			if (sessionId === "" || mode === void 0 || state.executor === void 0) throw new PluginError("bad-request", "executor/assign requires sessionId and mode");
			const owner = mode === "local" ? leaseOwner(lease) : void 0;
			return await state.executor.assign(sessionId, mode, owner, input["switchedBy"] === "user" || input["switchedBy"] === "default" || input["switchedBy"] === "handoff" ? input["switchedBy"] : "user");
		}
		case RW_METHODS.executorRelease: {
			const sessionId = asRecord(params)["sessionId"];
			if (typeof sessionId !== "string" || state.executor === void 0) throw new PluginError("bad-request", "executor/release requires sessionId");
			return await state.executor.assign(sessionId, "server", void 0, "user");
		}
		case RW_METHODS.executorPlanSwitch: {
			const input = asRecord(params);
			const sessionId = typeof input["sessionId"] === "string" ? input["sessionId"] : "";
			const to = input["to"] === "local" || input["to"] === "server" ? input["to"] : void 0;
			if (sessionId === "" || to === void 0) throw new PluginError("bad-request", "executor/plan-switch requires sessionId and to");
			const activeTurns = (ctx.get("agents")?.list() ?? []).filter((agent) => agent.status === "running").map((agent) => ({
				sessionId: String(agent.id),
				running: true,
				status: agent.status ?? "running"
			}));
			return {
				currentMode: (await state.executor?.get(sessionId))?.mode ?? "server",
				to,
				restartRequired: to === "local" ? state.managed?.unmanaged(sessionId) === true : false,
				activeTurns,
				requiresConfirmation: activeTurns.some((agent) => agent.sessionId !== sessionId),
				targetTurnRunning: activeTurns.some((agent) => agent.sessionId === sessionId)
			};
		}
		case RW_METHODS.executorSwitch: {
			const input = asRecord(params);
			const sessionId = typeof input["sessionId"] === "string" ? input["sessionId"] : "";
			const to = input["to"] === "local" || input["to"] === "server" ? input["to"] : void 0;
			if (sessionId === "" || to === void 0 || state.managed === void 0 || state.executor === void 0) throw new PluginError("bad-request", "executor/switch requires sessionId and to");
			if (to === "local" && input["confirmOtherTurns"] !== true) {
				const running = (ctx.get("agents")?.list() ?? []).filter((agent) => agent.status === "running" && String(agent.id) !== sessionId);
				if (running.length > 0) throw new PluginError("executor-switch-confirm-required", "other sessions are running; confirm before switching", { activeTurns: running.map((agent) => ({
					sessionId: String(agent.id),
					running: true,
					status: agent.status ?? "running"
				})) });
			}
			if (to === "server") {
				await state.executor.assign(sessionId, "server", void 0, "user");
				return {
					mode: "server",
					restartRequired: false
				};
			}
			return {
				mode: "local",
				restartRequired: (await state.managed.releaseForSwitch(sessionId, "local")).restartRequired
			};
		}
		case RW_METHODS.clientTargets: {
			const store = state.clientTargets;
			if (store === void 0) throw new PluginError("internal", "client/targets is not available in this process");
			return store.accept(params, { leaseClientId: lease.current?.clientId });
		}
		default: throw new PluginError("internal", `method ${method} is not implemented in this Phase A skeleton`, { method });
	}
}
/** Mount the v1 profile plugin. */
function apply(ctx, config = {}) {
	const paths = resolvePaths(ctx, config);
	if (paths === void 0) {
		console.error("[dsh-remote-workspace-server] no profile context; the v1 plugin must run inside a dsh profile");
		return;
	}
	const lease = new ProfileLease(paths.leaseFile, paths.profile, config.leaseHeartbeatMs ?? 5e3);
	const guardReady = Promise.withResolvers();
	setTimeout(() => {
		guardReady.resolve();
	}, 2e3).unref();
	const executor = new ExecutorStore(join(paths.home, "remote-workspace", "state", "executors"));
	const approvals = new ApprovalHub();
	ctx.effect(() => () => approvals.dispose(), "dsh-rw: approval hub lifetime");
	const qaApprovalTool = readQaApprovalTool(join(paths.home, "remote-workspace", paths.clientId, "qa-approval-tool"));
	const managed = new ManagedAgents(ctx, async (sessionId, next) => {
		await executor.assign(sessionId, next, leaseOwner(lease), "user");
	}, {
		approvals,
		approvalPreempt: qaApprovalTool !== void 0
	});
	const sessionLogCache = new SessionLogCache();
	const persist = new PersistBridge(ctx, executor, () => leaseOwner(lease), sessionLogCache);
	const markerPath = join(paths.home, "remote-workspace", paths.clientId, "qa-approval-tool");
	console.error(`[dsh-remote-workspace-server] approval gate: ${qaApprovalTool === void 0 ? "off" : `qa(${qaApprovalTool})`} marker=${markerPath}`);
	const state = {
		paths,
		pluginVersion: readPluginVersion(),
		titleFold: new TitleFold(ctx, sessionLogCache),
		clientTargets: new ClientTargetsSnapshot(),
		managed,
		executor,
		persist,
		approvals,
		approvalGate: qaApprovalTool === void 0 ? "off" : "qa",
		guardReady: guardReady.promise
	};
	ctx.effect(() => installQaApprovalGate(ctx, qaApprovalTool), "dsh-rw: QA approval gate");
	ctx.effect(() => () => lease.dispose(), "dsh-rw: lease heartbeat");
	ctx.effect(() => () => {
		managed.disposeAll();
	}, "dsh-rw: managed agents");
	ctx.effect(() => () => {
		persist.dispose();
	}, "dsh-rw: persist handles");
	let guard;
	ctx.effect(() => () => guard?.dispose(), "dsh-rw: registry guard lifetime");
	ctx.inject(["sessionController"], (scope) => {
		state.sessionController = scope.get("sessionController");
	});
	ctx.inject(["workspaceRegistry"], () => {
		guard ??= new RegistryGuard(ctx, paths.registryFile, join(paths.lockDir, "registry"));
		const current = guard;
		state.registerGuard = () => current.status();
		state.refreshRegistry = async () => await current.refresh();
		state.guardProbe = async () => await current.probe();
		state.guardReady = current.install().catch((error) => {
			console.error(`[dsh-remote-workspace-server] registry guard install failed: ${error instanceof Error ? error.message : String(error)}`);
		}).finally(() => guardReady.resolve());
	});
	installRoutes(ctx, config, state, lease);
}
//#endregion
export { DSH_VERIFIED_RUNTIME, apply, inject, name };
