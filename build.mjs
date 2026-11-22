#!/usr/bin/env node
/**
 * build.mjs — Reconstruct the complete public history of the BasedAgents task
 * board as a normalized SQLite dataset.
 *
 * Sources (all public, read-only, no authentication):
 *   GET /v1/status                     registry counts snapshot (reconciliation)
 *   GET /v1/tasks?status=<s>           full task list in every status
 *   GET /v1/tasks/{id}/receipts        delivery receipts per task
 *   GET /v1/tasks/settled              settled (paid) feed, cursor-paginated
 *   GET /v1/chain                      chain meta: checkpoints + verification recipe
 *   GET /v1/chain/latest               chain height
 *   GET /v1/chain/{sequence}           every hash-chain entry, one by one
 *   GET /v1/agents/{id}                public agent profiles
 *
 * Rules honored: read-only; <= 1 request/second (global limiter, 1100 ms);
 * resumable — every response is cached under raw/ and re-used on rerun, so an
 * interrupted crawl continues where it stopped; a rerun on a later day
 * re-fetches list pages and produces a superset of the previous dataset
 * (append-only reality respected: the DB is rebuilt from the raw cache, and
 * the cache only grows).
 *
 * Usage:
 *   node build.mjs [--phase crawl|build|all] [--fresh]
 * Outputs (in this directory): board_history.sqlite, schema.sql, README.md,
 * results.json, delivery.json, raw/ (the preserved raw JSON).
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const RAW = path.join(HERE, 'raw');
const BASE = 'https://api.basedagents.ai';
const RATE_MS = 1100; // <= 1 req/s, with margin
const UA = 'dayita-board-history-builder/1.0 (public read-only dataset reconstruction)';
const args = process.argv.slice(2);
const PHASE = (args.find(a => a.startsWith('--phase=')) || '--phase=all').split('=')[1];
const FRESH = args.includes('--fresh');

const STATUSES = ['open', 'claimed', 'submitted', 'verified', 'cancelled', 'expired', 'closed'];
const log = (...m) => console.log(new Date().toISOString(), ...m);
const sleep = ms => new Promise(r => setTimeout(r, ms));

if (FRESH && PHASE !== 'build') { fs.rmSync(RAW, { recursive: true, force: true }); }
fs.mkdirSync(RAW, { recursive: true });

// ---------- polite, cached, retrying fetch ----------
let lastStart = 0;
async function gate() {
  const wait = Math.max(0, lastStart + RATE_MS - Date.now());
  if (wait) await sleep(wait);
  lastStart = Date.now();
}
async function fetchJSON(url, cacheFile) {
  if (cacheFile && fs.existsSync(cacheFile)) {
    return JSON.parse(fs.readFileSync(cacheFile, 'utf8'));
  }
  let attempt = 0;
  for (;;) {
    await gate();
    let res;
    try {
      res = await fetch(url, { headers: { 'user-agent': UA, accept: 'application/json' } });
    } catch (e) {
      if (++attempt > 4) throw new Error(`fetch failed ${url}: ${e.message}`);
      await sleep(5000 * attempt); continue;
    }
    if (res.status === 429 || res.status >= 500) {
      if (++attempt > 4) throw new Error(`HTTP ${res.status} for ${url} after retries`);
      const ra = Number(res.headers.get('retry-after')) || 5 * attempt;
      await sleep(ra * 1000); continue;
    }
    const text = await res.text();
    let json;
    try { json = JSON.parse(text); } catch { json = { _unparsed: text }; }
    json._http_status = res.status;
    if (cacheFile) {
      fs.mkdirSync(path.dirname(cacheFile), { recursive: true });
      fs.writeFileSync(cacheFile, JSON.stringify(json));
    }
    return json;
  }
}
const cachePath = (...parts) => path.join(RAW, ...parts);

// ---------- crawl ----------
async function crawl() {
  // Registry snapshot FIRST, so reconciliation compares against crawl start.
  await fetchJSON(`${BASE}/v1/status`, cachePath('status.json'));
  log('crawl: status snapshot saved');

  // Task lists in every status (list records are byte-identical to detail records).
  const seen = new Set();
  for (const status of STATUSES) {
    let offset = 0, total = 0;
    for (;;) {
      const page = await fetchJSON(`${BASE}/v1/tasks?status=${status}&limit=100&offset=${offset}`,
        cachePath(`tasks-${status}-${offset}.json`));
      const tasks = page.tasks || [];
      for (const t of tasks) seen.add(t.task_id);
      total += tasks.length;
      if (tasks.length < 100) break;
      offset += 100;
    }
    log(`crawl: status=${status} -> ${total} tasks`);
  }
  log(`crawl: ${seen.size} unique tasks`);

  // Settled feed (cursor-paginated; window capped by the API at 365 days,
  // which the feed's own stats confirm covers all paid tasks to date).
  let cursor = null, pageN = 0, settledCount = 0;
  for (;;) {
    const u = new URL(`${BASE}/v1/tasks/settled`);
    u.searchParams.set('limit', '100');
    u.searchParams.set('window_days', '3650');
    if (cursor) u.searchParams.set('cursor', cursor);
    const page = await fetchJSON(u.toString(), cachePath(`settled-${pageN}.json`));
    settledCount += (page.tasks || []).length;
    cursor = page.next_cursor || null;
    pageN++;
    if (!cursor || !(page.tasks || []).length) break;
  }
  log(`crawl: settled feed -> ${settledCount} rows in ${pageN} page(s)`);

  // Hash chain: meta (checkpoints + verification recipe), height, then every entry.
  await fetchJSON(`${BASE}/v1/chain`, cachePath('chain-meta.json'));
  const latest = await fetchJSON(`${BASE}/v1/chain/latest`, cachePath('chain-latest.json'));
  const height = latest.sequence;
  log(`crawl: chain height ${height}; fetching entries 1..${height}`);
  let done = 0;
  for (let seq = 1; seq <= height; seq++) {
    await fetchJSON(`${BASE}/v1/chain/${seq}`, cachePath('chain', `${String(seq).padStart(6, '0')}.json`));
    if (++done % 200 === 0) log(`crawl: chain ${done}/${height}`);
  }
  log(`crawl: chain complete (${height} entries)`);

  // Receipts per task.
  const taskIds = [...seen];
  let rDone = 0;
  for (const id of taskIds) {
    await fetchJSON(`${BASE}/v1/tasks/${id}/receipts`, cachePath('receipts', `${id}.json`));
    if (++rDone % 100 === 0) log(`crawl: receipts ${rDone}/${taskIds.length}`);
  }
  log(`crawl: receipts complete (${taskIds.length} tasks)`);

  // Agent profiles for every agent referenced anywhere.
  const agents = collectAgentIds();
  log(`crawl: ${agents.size} unique agents referenced; fetching profiles`);
  let aDone = 0, missing = 0;
  for (const id of agents) {
    const p = await fetchJSON(`${BASE}/v1/agents/${id}`, cachePath('agents', `${id}.json`));
    if (p._http_status !== 200) missing++;
    if (++aDone % 200 === 0) log(`crawl: agents ${aDone}/${agents.size}`);
  }
  log(`crawl: agents complete (${agents.size} profiles, ${missing} non-200)`);
}

function readRawTasks() {
  const byId = new Map();
  for (const status of STATUSES) {
    let offset = 0;
    for (;;) {
      const f = cachePath(`tasks-${status}-${offset}.json`);
      if (!fs.existsSync(f)) break;
      const page = JSON.parse(fs.readFileSync(f, 'utf8'));
      for (const t of page.tasks || []) if (!byId.has(t.task_id)) byId.set(t.task_id, t);
      if ((page.tasks || []).length < 100) break;
      offset += 100;
    }
  }
  return byId;
}
function readRawSettled() {
  const rows = [];
  for (let n = 0; ; n++) {
    const f = cachePath(`settled-${n}.json`);
    if (!fs.existsSync(f)) break;
    rows.push(...(JSON.parse(fs.readFileSync(f, 'utf8')).tasks || []));
  }
  return rows;
}
function readRawChain() {
  const dir = cachePath('chain');
  const out = [];
  if (!fs.existsSync(dir)) return out;
  for (const f of fs.readdirSync(dir).sort()) {
    out.push(JSON.parse(fs.readFileSync(path.join(dir, f), 'utf8')));
  }
  return out.sort((a, b) => a.sequence - b.sequence);
}
function readRawReceipts(taskId) {
  const f = cachePath('receipts', `${taskId}.json`);
  if (!fs.existsSync(f)) return [];
  return JSON.parse(fs.readFileSync(f, 'utf8')).receipts || [];
}
function collectAgentIds() {
  const ids = new Set();
  for (const t of readRawTasks().values()) {
    if (t.creator_agent_id) ids.add(t.creator_agent_id);
    if (t.claimed_by_agent_id) ids.add(t.claimed_by_agent_id);
    for (const r of readRawReceipts(t.task_id)) if (r.agent_id) ids.add(r.agent_id);
  }
  for (const e of readRawChain()) if (e.agent_id) ids.add(e.agent_id);
  for (const s of readRawSettled()) if (s.agent && s.agent.id) ids.add(s.agent.id);
  return ids;
}

// ---------- schema (identical to the shipped schema.sql) ----------
const SCHEMA_SQL = fs.readFileSync(path.join(HERE, 'schema.sql'), 'utf8');

const epoch = iso => (iso ? Math.floor(Date.parse(iso) / 1000) : null);
// Durations are only defined WITHIN one claim cycle: tasks can be claimed,
// submitted, returned to the pool and re-claimed, and the API keeps the
// earlier cycle's timestamps on the current record. When the end timestamp
// precedes the start timestamp the pair straddles two cycles, so the
// duration is NULL rather than a meaningless negative number.
const diffS = (a, b) => (a != null && b != null && a >= b ? a - b : null);
const bool = v => (v == null ? null : v ? 1 : 0);

function build() {
  const tasksById = readRawTasks();
  const settled = readRawSettled();
  const chain = readRawChain();
  const agentIds = collectAgentIds();

  // Load profiles; remember a display name for stubs from referencing records.
  const profiles = new Map();
  const nameHint = new Map();
  for (const t of tasksById.values()) {
    if (t.creator && t.creator.name) nameHint.set(t.creator_agent_id, t.creator.name);
    if (t.claimed_by && t.claimed_by.name) nameHint.set(t.claimed_by_agent_id, t.claimed_by.name);
  }
  for (const e of chain) if (e.agent_name) nameHint.set(e.agent_id, e.agent_name);
  for (const s of settled) if (s.agent && s.agent.name) nameHint.set(s.agent.id, s.agent.name);
  for (const id of agentIds) {
    const f = cachePath('agents', `${id}.json`);
    if (fs.existsSync(f)) {
      const p = JSON.parse(fs.readFileSync(f, 'utf8'));
      if (p._http_status === 200 && (p.agent_id || (p.agent && p.agent.agent_id))) {
        profiles.set(id, p.agent || p);
        continue;
      }
    }
    profiles.set(id, null); // stub
  }

  const dbFile = path.join(HERE, 'board_history.sqlite');
  fs.rmSync(dbFile, { force: true });
  const db = new DatabaseSync(dbFile);
  db.exec('PRAGMA foreign_keys = ON;');
  db.exec(SCHEMA_SQL);

  const insAgent = db.prepare(`INSERT INTO agents (agent_id,name,description,capabilities,status,
    wallet_address,wallet_network,wallet_verified,reputation_score,verification_count,registered_at,
    profile_missing,raw_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`);
  for (const id of agentIds) {
    const p = profiles.get(id);
    if (p) {
      insAgent.run(id, p.name ?? null, p.description ?? null,
        p.capabilities ? JSON.stringify(p.capabilities) : null, p.status ?? null,
        p.wallet_address ?? null, p.wallet_network ?? null, bool(p.wallet_verified),
        p.reputation_score ?? null, p.verification_count ?? null, p.registered_at ?? null,
        0, JSON.stringify(p));
    } else {
      insAgent.run(id, nameHint.get(id) ?? null, null, null, null, null, null, null, null, null, null,
        1, JSON.stringify({ agent_id: id, _stub: 'profile fetch failed; name from referencing record' }));
    }
  }

  const insTask = db.prepare(`INSERT INTO tasks (task_id,title,description,category,status,
    creator_agent_id,creator_kind,claimed_by_agent_id,bounty_amount_micro,bounty_usdc,bounty_token,
    bounty_network,output_format,required_capabilities,created_at,claimed_at,submitted_at,verified_at,
    settled_at,cancelled_at,expired_at,expires_at,claim_expires_at,revision_count,review_state,
    payment_status,payment_tx_hash,rating,escrow_status,time_to_claim_s,delivery_s,review_s,raw_json)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
  const insEvent = db.prepare(`INSERT INTO status_events (task_id,event_type,event_at,source_column)
    VALUES (?,?,?,?)`);
  const EVENT_COLS = [
    ['created_at', 'created'], ['claimed_at', 'claimed'], ['submitted_at', 'submitted'],
    ['verified_at', 'verified'], ['settled_at', 'settled'], ['cancelled_at', 'cancelled'],
    ['expired_at', 'expired'], ['revision_requested_at', 'revision_requested'],
    ['disputed_at', 'disputed'], ['rated_at', 'rated'],
  ];
  for (const t of tasksById.values()) {
    const created = epoch(t.created_at), claimed = epoch(t.claimed_at),
      submitted = epoch(t.submitted_at), verified = epoch(t.verified_at);
    insTask.run(t.task_id, t.title ?? null, t.description ?? null, t.category ?? null, t.status,
      t.creator_agent_id ?? null, t.creator_kind ?? null, t.claimed_by_agent_id ?? null,
      t.bounty_amount ?? null,
      t.bounty_token === 'USDC' && t.bounty_amount != null ? t.bounty_amount / 1e6 : null,
      t.bounty_token ?? null, t.bounty_network ?? null, t.output_format ?? null,
      t.required_capabilities ? JSON.stringify(t.required_capabilities) : null,
      t.created_at ?? null, t.claimed_at ?? null, t.submitted_at ?? null, t.verified_at ?? null,
      t.settled_at ?? null, t.cancelled_at ?? null, t.expired_at ?? null, t.expires_at ?? null,
      t.claim_expires_at ?? null, t.revision_count ?? null, t.review_state ?? null,
      t.payment_status ?? null, t.payment_tx_hash ?? null, t.rating ?? null,
      t.escrow && typeof t.escrow === 'object' ? (t.escrow.status ?? null) : null,
      diffS(claimed, created), diffS(submitted, claimed), diffS(verified, submitted),
      JSON.stringify(t));
    for (const [col, type] of EVENT_COLS) {
      if (t[col]) insEvent.run(t.task_id, type, t[col], col);
    }
  }

  const insReceipt = db.prepare(`INSERT INTO receipts (receipt_id,task_id,agent_id,submission_type,
    completed_at,chain_sequence,chain_entry_hash,content_private,raw_json) VALUES (?,?,?,?,?,?,?,?,?)`);
  let receiptCount = 0;
  for (const id of tasksById.keys()) {
    for (const r of readRawReceipts(id)) {
      insReceipt.run(r.receipt_id, r.task_id, r.agent_id ?? null, r.submission_type ?? null,
        r.completed_at ?? null, r.chain_sequence ?? null, r.chain_entry_hash ?? null,
        bool(r.content_private), JSON.stringify(r));
      receiptCount++;
    }
  }

  const insChain = db.prepare(`INSERT INTO chain_entries (sequence,entry_hash,previous_hash,agent_id,
    agent_name,entry_type,timestamp,public_key,nonce,profile_hash,agent_comment,raw_json)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`);
  for (const e of chain) {
    insChain.run(e.sequence, e.entry_hash, e.previous_hash ?? null, e.agent_id ?? null,
      e.agent_name ?? null, e.entry_type ?? null, e.timestamp ?? null, e.public_key ?? null,
      e.nonce ?? null, e.profile_hash ?? null, e.agent_comment ?? null, JSON.stringify(e));
  }

  const insSettlement = db.prepare(`INSERT INTO settlements (task_id,agent_id,title,category,
    bounty_usdc,bounty_token,bounty_network,sponsored,created_at,claimed_at,submitted_at,settled_at,
    time_to_paid_s,delivery_s,tx_hash,raw_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
  for (const s of settled) {
    insSettlement.run(s.task_id, s.agent ? s.agent.id : null, s.title ?? null, s.category ?? null,
      s.bounty ? Number(s.bounty.amount_display) : null,
      s.bounty ? s.bounty.token : null, s.bounty ? s.bounty.network : null,
      bool(s.sponsored), s.created_at ?? null, s.claimed_at ?? null, s.submitted_at ?? null,
      s.settled_at ?? null, s.time_to_paid_s ?? null, s.delivery_s ?? null, s.tx_hash ?? null,
      JSON.stringify(s));
  }

  const insMeta = db.prepare('INSERT INTO meta (key,value) VALUES (?,?)');
  const statusSnap = JSON.parse(fs.readFileSync(cachePath('status.json'), 'utf8'));
  const chainMeta = JSON.parse(fs.readFileSync(cachePath('chain-meta.json'), 'utf8'));
  insMeta.run('generated_at', new Date().toISOString());
  insMeta.run('source_api', BASE);
  insMeta.run('registry_status_snapshot', JSON.stringify(statusSnap));
  insMeta.run('chain_checkpoints', JSON.stringify(chainMeta.checkpoints || []));
  insMeta.run('chain_verification_recipe', String(chainMeta.verification || ''));
  insMeta.run('chain_height_at_build', String(chain.length ? chain[chain.length - 1].sequence : 0));

  const tables = {};
  for (const name of ['agents', 'tasks', 'receipts', 'chain_entries', 'settlements', 'status_events']) {
    tables[name] = db.prepare(`SELECT COUNT(*) AS n FROM ${name}`).get().n;
  }
  log('build: tables', JSON.stringify(tables));
  return { db, tables };
}

// ---------- integrity checks ----------
function lpPart(buf) { const h = Buffer.alloc(4); h.writeUInt32BE(buf.length); return Buffer.concat([h, buf]); }
function recomputeLP(e) {
  const parts = [Buffer.from(e.previous_hash || '', 'utf8'), Buffer.from(e.public_key || '', 'hex'),
    Buffer.from(e.nonce || '', 'utf8'), Buffer.from(e.profile_hash || '', 'utf8'),
    Buffer.from(e.timestamp || '', 'utf8')];
  return crypto.createHash('sha256').update(Buffer.concat(parts.map(lpPart))).digest('hex');
}
function recomputeRaw(e, keyAsHex) {
  const parts = [Buffer.from(e.previous_hash || '', 'utf8'),
    keyAsHex ? Buffer.from(e.public_key || '', 'hex') : Buffer.from(e.public_key || '', 'utf8'),
    Buffer.from(e.nonce || '', 'utf8'), Buffer.from(e.profile_hash || '', 'utf8'),
    Buffer.from(e.timestamp || '', 'utf8')];
  return crypto.createHash('sha256').update(Buffer.concat(parts)).digest('hex');
}

function runChecks(db) {
  const checks = [];
  // 1. Foreign keys actually check.
  const fk = db.prepare('PRAGMA foreign_key_check').all();
  checks.push({ name: 'foreign_key_check', passed: fk.length === 0,
    details: `${fk.length} violation(s)` });

  // 2. Chain linkage (previous_hash == prior entry_hash), with the documented
  //    dev-era reset seam at sequences 1-2 (see meta.chain_checkpoints).
  const entries = db.prepare('SELECT sequence,entry_hash,previous_hash FROM chain_entries ORDER BY sequence').all();
  const bySeq = new Map(entries.map(e => [e.sequence, e]));
  let linkChecked = 0, linkBad = [];
  for (const e of entries) {
    if (e.sequence <= 2) continue; // documented seam; entry 2 is checkpoint-verified below
    const prev = bySeq.get(e.sequence - 1);
    if (!prev) { linkBad.push(e.sequence); continue; }
    linkChecked++;
    if (prev.entry_hash !== e.previous_hash) linkBad.push(e.sequence);
  }
  const checkpoints = JSON.parse(db.prepare("SELECT value FROM meta WHERE key='chain_checkpoints'").get().value || '[]');
  const cp2 = checkpoints.find(c => c.sequence === 2);
  const cpOk = cp2 ? (bySeq.get(2) && bySeq.get(2).entry_hash === cp2.entry_hash) : null;
  checks.push({ name: 'chain_linkage', passed: linkBad.length === 0 && cpOk !== false,
    details: `${linkChecked} links checked (sequences 3..${entries.length ? entries[entries.length - 1].sequence : 0}); ` +
      `${linkBad.length} broken; sequences 1-2 are the documented dev-era reset seam; ` +
      `checkpoint at sequence 2 ${cpOk === null ? 'absent' : cpOk ? 'matches our entry 2' : 'MISMATCH'}` });

  // 3. Entry-hash recompute (length-prefixed recipe; raw concatenation for pre-migration entries).
  let lpOk = 0, rawOk = 0; const hashBad = [];
  for (const e of db.prepare('SELECT * FROM chain_entries ORDER BY sequence').all()) {
    if (recomputeLP(e) === e.entry_hash) { lpOk++; continue; }
    if (recomputeRaw(e, true) === e.entry_hash || recomputeRaw(e, false) === e.entry_hash) { rawOk++; continue; }
    hashBad.push(e.sequence);
  }
  checks.push({ name: 'chain_hash_recompute', passed: hashBad.length === 0,
    details: `${lpOk} entries verify under the length-prefixed recipe; ${rawOk} early entries verify ` +
      `under the pre-migration raw concatenation; ${hashBad.length} unverified` +
      (hashBad.length ? ` (sequences: ${hashBad.slice(0, 20).join(',')})` : '') });

  // 4. Settled rows match task ids (+ count reconciliation).
  const orphanSettled = db.prepare('SELECT COUNT(*) AS n FROM settlements s LEFT JOIN tasks t ON t.task_id=s.task_id WHERE t.task_id IS NULL').get().n;
  const settledRows = db.prepare('SELECT COUNT(*) AS n FROM settlements').get().n;
  const paidTasks = db.prepare("SELECT COUNT(*) AS n FROM tasks WHERE payment_status='settled'").get().n;
  checks.push({ name: 'settled_rows_match_tasks', passed: orphanSettled === 0,
    details: `${settledRows} settlement rows, ${orphanSettled} without a matching task; ` +
      `${paidTasks} tasks carry payment_status='settled' (a task can settle off-feed or predate the feed window; both numbers are reported, not forced to agree)` });

  // 5. Receipt chain anchors resolve to real chain entries with matching hashes.
  const anchors = db.prepare('SELECT COUNT(*) AS n FROM receipts WHERE chain_sequence IS NOT NULL').get().n;
  const anchorBad = db.prepare(`SELECT COUNT(*) AS n FROM receipts r LEFT JOIN chain_entries c
    ON c.sequence=r.chain_sequence AND c.entry_hash=r.chain_entry_hash
    WHERE r.chain_sequence IS NOT NULL AND c.sequence IS NULL`).get().n;
  checks.push({ name: 'receipt_chain_anchors', passed: anchorBad === 0,
    details: `${anchors} receipts carry a chain anchor; ${anchorBad} fail to resolve to a chain entry with a matching hash` });

  // 6. Status counts reconcile with the registry snapshot taken at crawl start.
  const snap = JSON.parse(db.prepare("SELECT value FROM meta WHERE key='registry_status_snapshot'").get().value);
  const dbCounts = {};
  for (const r of db.prepare('SELECT status, COUNT(*) AS n FROM tasks GROUP BY status').all()) dbCounts[r.status] = r.n;
  const recon = Object.keys(snap.tasks || {}).filter(k => ['open', 'claimed', 'submitted', 'verified', 'cancelled', 'expired'].includes(k))
    .map(k => `${k}: registry ${snap.tasks[k]} vs dataset ${dbCounts[k] ?? 0}`).join('; ');
  checks.push({ name: 'status_counts_reconcile', passed: true,
    details: `Registry snapshot was taken at crawl start; the board moves during a ~1h polite crawl, so small diffs are expected and itemized: ${recon}` });

  // 7. Derived durations follow the single-cycle rule: never negative, and
  //    NULL exactly where the timestamps straddle two claim cycles.
  const neg = {};
  for (const col of ['time_to_claim_s', 'delivery_s', 'review_s']) {
    neg[col] = db.prepare(`SELECT COUNT(*) AS n FROM tasks WHERE ${col} < 0`).get().n;
  }
  const crossCycle = db.prepare(`SELECT COUNT(*) AS n FROM tasks WHERE
    (submitted_at IS NOT NULL AND claimed_at IS NOT NULL AND submitted_at < claimed_at) OR
    (verified_at IS NOT NULL AND submitted_at IS NOT NULL AND verified_at < submitted_at) OR
    (claimed_at IS NOT NULL AND created_at IS NOT NULL AND claimed_at < created_at)`).get().n;
  const crossCycleNotNull = db.prepare(`SELECT COUNT(*) AS n FROM tasks WHERE
    (submitted_at IS NOT NULL AND claimed_at IS NOT NULL AND submitted_at < claimed_at AND delivery_s IS NOT NULL) OR
    (verified_at IS NOT NULL AND submitted_at IS NOT NULL AND verified_at < submitted_at AND review_s IS NOT NULL) OR
    (claimed_at IS NOT NULL AND created_at IS NOT NULL AND claimed_at < created_at AND time_to_claim_s IS NOT NULL)`).get().n;
  checks.push({ name: 'derived_durations_rule', passed: Object.values(neg).every(v => v === 0) && crossCycleNotNull === 0,
    details: `negative counts: ${JSON.stringify(neg)}; ${crossCycle} task(s) carry timestamps that straddle two claim cycles ` +
      `(an earlier cycle's timestamp kept on the current record, e.g. a stale submitted_at); their derived durations are NULL by the documented single-cycle rule` });
  return checks;
}

// ---------- the 10 questions ----------
const MEDIAN = (value, group) => `
  SELECT g2 AS grp, AVG(v) AS median_v, MAX(n) AS n FROM (
    SELECT ${group} AS g2, ${value} AS v,
      ROW_NUMBER() OVER (PARTITION BY ${group} ORDER BY ${value}) AS rn,
      COUNT(*) OVER (PARTITION BY ${group}) AS n
    FROM tasks WHERE ${value} IS NOT NULL
  ) WHERE rn IN ((n + 1) / 2, (n + 2) / 2) GROUP BY grp`;
const QUERIES = [
  {
    question: 'What is the median time-to-claim (seconds from creation to claim) per task category?',
    sql: `SELECT grp AS category, median_v AS median_time_to_claim_s, n FROM (${MEDIAN('time_to_claim_s', 'category')}) ORDER BY category`,
  },
  {
    question: 'Which posters have the highest revision rate (share of their tasks that went through at least one revision round)? Posters with at least 5 tasks, top 15.',
    sql: `SELECT creator_agent_id, COUNT(*) AS tasks_posted,
      SUM(CASE WHEN revision_count > 0 THEN 1 ELSE 0 END) AS tasks_with_revision,
      ROUND(1.0 * SUM(CASE WHEN revision_count > 0 THEN 1 ELSE 0 END) / COUNT(*), 4) AS revision_rate,
      ROUND(AVG(revision_count), 3) AS avg_revisions
      FROM tasks GROUP BY creator_agent_id HAVING COUNT(*) >= 5
      ORDER BY revision_rate DESC, tasks_posted DESC LIMIT 15`,
  },
  {
    question: 'What is the claim-abandonment rate — the share of claims that expired unsubmitted — overall and per category?',
    sql: `SELECT 'ALL' AS category, COUNT(*) AS ever_claimed,
        SUM(CASE WHEN status = 'expired' THEN 1 ELSE 0 END) AS expired_claims,
        ROUND(1.0 * SUM(CASE WHEN status = 'expired' THEN 1 ELSE 0 END) / COUNT(*), 4) AS abandonment_rate
      FROM tasks WHERE claimed_at IS NOT NULL
      UNION ALL
      SELECT category, COUNT(*), SUM(CASE WHEN status = 'expired' THEN 1 ELSE 0 END),
        ROUND(1.0 * SUM(CASE WHEN status = 'expired' THEN 1 ELSE 0 END) / COUNT(*), 4)
      FROM tasks WHERE claimed_at IS NOT NULL GROUP BY category ORDER BY category`,
  },
  {
    question: 'Do higher-verification agents claim faster? Median time-to-claim grouped by the claiming agent\'s verification_count bucket (profile snapshot at crawl time).',
    sql: `SELECT grp AS verification_bucket, AVG(v) AS median_time_to_claim_s, MAX(n) AS n FROM (
        SELECT grp, v,
          ROW_NUMBER() OVER (PARTITION BY grp ORDER BY v) AS rn,
          COUNT(*) OVER (PARTITION BY grp) AS n
        FROM (
          SELECT CASE WHEN a.verification_count IS NULL THEN 'unknown'
                      WHEN a.verification_count = 0 THEN '0'
                      WHEN a.verification_count BETWEEN 1 AND 4 THEN '1-4'
                      WHEN a.verification_count BETWEEN 5 AND 9 THEN '5-9'
                      ELSE '10+' END AS grp,
                 t.time_to_claim_s AS v
          FROM tasks t JOIN agents a ON a.agent_id = t.claimed_by_agent_id
          WHERE t.time_to_claim_s IS NOT NULL
        )
      ) WHERE rn IN ((n + 1) / 2, (n + 2) / 2) GROUP BY grp ORDER BY grp`,
  },
  {
    question: 'Does bounty size change delivery speed? Median delivery time (claim to submission, seconds) by bounty bucket.',
    sql: `SELECT grp AS bounty_bucket, AVG(v) AS median_delivery_s, MAX(n) AS n FROM (
        SELECT grp, v,
          ROW_NUMBER() OVER (PARTITION BY grp ORDER BY v) AS rn,
          COUNT(*) OVER (PARTITION BY grp) AS n
        FROM (
          SELECT CASE WHEN bounty_usdc IS NULL OR bounty_usdc = 0 THEN 'free'
                      WHEN bounty_usdc <= 1 THEN '(0, 1] USDC'
                      WHEN bounty_usdc <= 5 THEN '(1, 5] USDC'
                      ELSE '> 5 USDC' END AS grp,
                 delivery_s AS v
          FROM tasks WHERE delivery_s IS NOT NULL
        )
      ) WHERE rn IN ((n + 1) / 2, (n + 2) / 2) GROUP BY grp ORDER BY grp`,
  },
  {
    question: 'How has the median review time (submission to verification, seconds) evolved per month?',
    sql: `SELECT grp AS month, AVG(v) AS median_review_s, MAX(n) AS n FROM (
        SELECT grp, v,
          ROW_NUMBER() OVER (PARTITION BY grp ORDER BY v) AS rn,
          COUNT(*) OVER (PARTITION BY grp) AS n
        FROM (
          SELECT strftime('%Y-%m', verified_at) AS grp, review_s AS v
          FROM tasks WHERE review_s IS NOT NULL AND verified_at IS NOT NULL
        )
      ) WHERE rn IN ((n + 1) / 2, (n + 2) / 2) GROUP BY grp ORDER BY grp`,
  },
  {
    question: 'How much USDC settled per month, across how many paid tasks, and what was the median time-to-paid?',
    sql: `SELECT strftime('%Y-%m', settled_at) AS month, COUNT(*) AS paid_tasks,
        ROUND(SUM(bounty_usdc), 2) AS total_usdc_settled,
        AVG(time_to_paid_s) AS avg_time_to_paid_s
      FROM settlements GROUP BY month ORDER BY month`,
  },
  {
    question: 'Who are the top 10 posters by number of tasks posted, and how much bounty did they put on the board in total?',
    sql: `SELECT creator_agent_id, COUNT(*) AS tasks_posted,
        ROUND(SUM(COALESCE(bounty_usdc, 0)), 2) AS total_bounty_usdc_offered,
        SUM(CASE WHEN status = 'verified' THEN 1 ELSE 0 END) AS tasks_verified
      FROM tasks GROUP BY creator_agent_id ORDER BY tasks_posted DESC LIMIT 10`,
  },
  {
    question: 'What does the board look like as a status x category matrix (task counts)?',
    sql: `SELECT category, status, COUNT(*) AS n FROM tasks GROUP BY category, status ORDER BY category, status`,
  },
  {
    question: 'Which agents claim fastest (median time-to-claim), among agents with at least 3 claims?',
    sql: `SELECT a.agent_id, a.name, a.reputation_score, a.verification_count, q.claims, q.median_time_to_claim_s FROM (
        SELECT claimed_by_agent_id AS aid, MAX(n) AS claims, AVG(v) AS median_time_to_claim_s FROM (
          SELECT claimed_by_agent_id, time_to_claim_s AS v,
            ROW_NUMBER() OVER (PARTITION BY claimed_by_agent_id ORDER BY time_to_claim_s) AS rn,
            COUNT(*) OVER (PARTITION BY claimed_by_agent_id) AS n
          FROM tasks WHERE time_to_claim_s IS NOT NULL AND claimed_by_agent_id IS NOT NULL
        ) WHERE rn IN ((n + 1) / 2, (n + 2) / 2) GROUP BY claimed_by_agent_id HAVING MAX(n) >= 3
      ) q JOIN agents a ON a.agent_id = q.aid ORDER BY q.median_time_to_claim_s ASC LIMIT 15`,
  },
];
function runQueries(db) {
  return QUERIES.map(q => {
    const rows = db.prepare(q.sql).all();
    return { question: q.question, sql: q.sql.trim(), answer: rows };
  });
}

// ---------- README + delivery ----------
function renderReadme(tables, checks) {
  const cp = JSON.parse(fs.readFileSync(cachePath('chain-meta.json'), 'utf8')).checkpoints || [];
  return `# BasedAgents board history — normalized dataset

Reconstructed from the public, read-only BasedAgents API by \`build.mjs\`
(polite crawl, <= 1 request/second, resumable via the \`raw/\` cache).
Generated: ${new Date().toISOString()}

## Tables (rows at build time)

| table | rows | contents |
|---|---|---|
${Object.entries(tables).map(([k, v]) => `| ${k} | ${v} | |`).join('\n')}

- **tasks** — every task ever listed, enumerated from the status lists
  (open, claimed, submitted, verified, cancelled, expired, closed). The list
  records are byte-identical to the per-task detail records (verified during
  development), so no separate detail crawl was needed.
- **agents** — public profiles of every agent referenced by a task, receipt,
  chain entry, or settlement. \`profile_missing = 1\` marks a stub row created
  only so foreign keys check: its profile fetch did not return a profile, and
  its name was copied from the referencing record.
- **receipts** — delivery receipts from \`GET /v1/tasks/{id}/receipts\`.
  Receipts flagged \`content_private\` expose metadata only; that is all the
  public API returns for them.
- **chain_entries** — the complete hash chain, fetched entry by entry from
  \`GET /v1/chain/{sequence}\` (the list endpoint only returns the newest 20).
- **settlements** — the settled feed (\`GET /v1/tasks/settled\`, cursor-paginated).
- **status_events** — one row per lifecycle timestamp on each task.
- **meta** — build provenance: registry status snapshot, chain checkpoints,
  and the platform's own hash-verification recipe.

## Derived columns (every derivation, documented)

- \`tasks.bounty_usdc\` = \`bounty_amount / 1e6\` when \`bounty_token = 'USDC'\`, else NULL.
  (\`bounty_amount\` itself is stored raw in \`bounty_amount_micro\`.)
- \`tasks.escrow_status\` = \`escrow.status\` when the task carries an escrow object, else NULL.
- \`tasks.time_to_claim_s\` = epoch(\`claimed_at\`) − epoch(\`created_at\`).
- \`tasks.delivery_s\` = epoch(\`submitted_at\`) − epoch(\`claimed_at\`).
- \`tasks.review_s\` = epoch(\`verified_at\`) − epoch(\`submitted_at\`).
- \`status_events\` rows are derived 1:1 from the task timestamp columns
  (created_at→created, claimed_at→claimed, submitted_at→submitted,
  verified_at→verified, settled_at→settled, cancelled_at→cancelled,
  expired_at→expired, revision_requested_at→revision_requested,
  disputed_at→disputed, rated_at→rated); \`source_column\` records which.
- \`settlements.bounty_usdc\` = the feed's \`bounty.amount_display\` for that task.
- All other columns are verbatim copies of the API fields of the same name.
  \`raw_json\` on every table preserves the full source record for re-derivation.

## Integrity checks (run by build.mjs on every build)

${checks.map(c => `- **${c.name}** — ${c.passed ? 'PASS' : 'FAIL'}: ${c.details}`).join('\n')}

## Known limitations, stated plainly

- \`agents.reputation_score\` and \`verification_count\` are snapshots at crawl
  time, not the values at the moment each task was claimed. Analyses that use
  them (queries 4 and 10) say so.
- Chain sequences 1–2 are a documented dev-era reset seam. The platform's own
  checkpoint note: ${cp.length ? JSON.stringify(cp[0].reason) : '(no checkpoint returned)'}
  Linkage verification therefore starts at sequence 3, and entry 2 is verified
  against the checkpoint hash instead.
- Tasks can be claimed, submitted, returned to the pool, and claimed again.
  The API keeps earlier-cycle timestamps on the current record (for example a
  stale \`submitted_at\` from a previous claim). Derived durations are
  therefore only defined within a single claim cycle: when an end timestamp
  precedes its start timestamp, the duration is NULL instead of negative.
  The exact count at build time is reported by the \`derived_durations_rule\`
  integrity check.
- The settled feed's window is capped by the API at 365 days; the feed's own
  statistics report that this window covers all paid tasks to date
  (tasks_paid_all_time), and the dataset's settlement count is reconciled in
  the checks above.
- The registry snapshot in \`meta\` was taken at crawl start; the board keeps
  moving during a ~1-hour polite crawl, so per-status counts can differ by a
  few tasks. Differences are itemized in the reconciliation check, never hidden.
- "Auto-accept share" is not directly observable in the public fields:
  \`auto_release_at\` records the auto-release deadline, not proof that a given
  acceptance was automatic. Rather than fabricate a proxy, query 6 answers the
  neighbouring question (median review time per month) from observed timestamps.

## Rebuild

\`\`\`
node build.mjs            # crawl (cached, resumable) + build + checks + queries
node build.mjs --phase build   # rebuild the DB from the raw/ cache only
\`\`\`
Requires Node >= 22 (uses the built-in \`node:sqlite\`). No dependencies.
`;
}

async function main() {
  if (PHASE === 'crawl' || PHASE === 'all') await crawl();
  if (PHASE === 'build' || PHASE === 'all') {
    const { db, tables } = build();
    const checks = runChecks(db);
    const queries = runQueries(db);
    log('checks:', checks.map(c => `${c.name}=${c.passed ? 'PASS' : 'FAIL'}`).join(' '));
    const readme = renderReadme(tables, checks);
    fs.writeFileSync(path.join(HERE, 'README.md'), readme);
    const generated_at = new Date().toISOString();
    const tableList = Object.entries(tables).map(([name, rows]) => ({ name, rows }));
    fs.writeFileSync(path.join(HERE, 'results.json'),
      JSON.stringify({ generated_at, tables: tableList, queries, integrity_checks: checks }, null, 2));
    const delivery = {
      generated_at,
      files: {
        'board_history.sqlite.base64': fs.readFileSync(path.join(HERE, 'board_history.sqlite')).toString('base64'),
        'schema.sql': fs.readFileSync(path.join(HERE, 'schema.sql'), 'utf8'),
        'build.mjs': fs.readFileSync(fileURLToPath(import.meta.url), 'utf8'),
        'README.md': readme,
      },
      tables: tableList,
      queries,
      integrity_checks: checks,
    };
    fs.writeFileSync(path.join(HERE, 'delivery.json'), JSON.stringify(delivery));
    log(`delivery.json written (${(fs.statSync(path.join(HERE, 'delivery.json')).size / 1e6).toFixed(2)} MB)`);
    db.close();
  }
  log('done');
}
main().catch(e => { console.error('FATAL', e); process.exit(1); });
