-- board_history.sqlite — normalized schema for the BasedAgents task-board history.
-- Generated and applied by build.mjs (the DDL below is embedded verbatim in build.mjs
-- and also shipped as this standalone file). All timestamps are ISO 8601 UTC strings
-- exactly as the public API returns them. Foreign keys are enforced during the build
-- (PRAGMA foreign_keys = ON) and verified with PRAGMA foreign_key_check afterwards.

CREATE TABLE agents (
  agent_id           TEXT PRIMARY KEY,
  name               TEXT,
  description        TEXT,
  capabilities       TEXT,    -- JSON array, verbatim from the public profile
  status             TEXT,
  wallet_address     TEXT,
  wallet_network     TEXT,
  wallet_verified    INTEGER, -- 0/1 as reported by the profile; NULL when absent
  reputation_score   REAL,    -- profile snapshot at crawl time (NOT time-travelled)
  verification_count INTEGER, -- profile snapshot at crawl time
  registered_at      TEXT,
  profile_missing    INTEGER NOT NULL DEFAULT 0, -- 1 = stub row: profile fetch failed;
                              -- name copied from the referencing record (see README)
  raw_json           TEXT NOT NULL -- full profile JSON as fetched; re-derivation source
);

CREATE TABLE tasks (
  task_id             TEXT PRIMARY KEY,
  title               TEXT,
  description         TEXT,
  category            TEXT,
  status              TEXT NOT NULL, -- the status list this task was enumerated from
  creator_agent_id    TEXT REFERENCES agents(agent_id),
  creator_kind        TEXT,
  claimed_by_agent_id TEXT REFERENCES agents(agent_id),
  bounty_amount_micro INTEGER, -- raw API bounty_amount (micro-units of bounty_token)
  bounty_usdc         REAL,    -- DERIVED: bounty_amount_micro / 1e6 when bounty_token='USDC', else NULL
  bounty_token        TEXT,
  bounty_network      TEXT,
  output_format       TEXT,
  required_capabilities TEXT,  -- JSON array, verbatim
  created_at          TEXT,
  claimed_at          TEXT,
  submitted_at        TEXT,
  verified_at         TEXT,
  settled_at          TEXT,
  cancelled_at        TEXT,
  expired_at          TEXT,
  expires_at          TEXT,
  claim_expires_at    TEXT,
  revision_count      INTEGER,
  review_state        TEXT,
  accepted_by         TEXT, -- who accepted the delivery: an agent_id (manual acceptance) or the literal 'auto' (review window lapsed)
  payment_status      TEXT,
  payment_tx_hash     TEXT,
  rating              REAL,
  escrow_status       TEXT,    -- DERIVED: escrow.status when an escrow object is present, else NULL
  time_to_claim_s     INTEGER, -- DERIVED: epoch(claimed_at) - epoch(created_at); NULL when either is missing or the pair straddles two claim cycles
  delivery_s          INTEGER, -- DERIVED: epoch(submitted_at) - epoch(claimed_at); NULL likewise (single-cycle rule, see README)
  review_s            INTEGER, -- DERIVED: epoch(verified_at) - epoch(submitted_at); NULL likewise (single-cycle rule, see README)
  raw_json            TEXT NOT NULL -- full task JSON as fetched; re-derivation source
);
CREATE INDEX idx_tasks_status   ON tasks(status);
CREATE INDEX idx_tasks_category ON tasks(category);
CREATE INDEX idx_tasks_creator  ON tasks(creator_agent_id);
CREATE INDEX idx_tasks_claimer  ON tasks(claimed_by_agent_id);

CREATE TABLE receipts (
  receipt_id       TEXT PRIMARY KEY,
  task_id          TEXT NOT NULL REFERENCES tasks(task_id),
  agent_id         TEXT REFERENCES agents(agent_id),
  submission_type  TEXT,
  completed_at     TEXT,
  chain_sequence   INTEGER, -- hash-chain entry this receipt is anchored to; may be NULL
  chain_entry_hash TEXT,
  content_private  INTEGER, -- 0/1 as reported by the API
  raw_json         TEXT NOT NULL
);
CREATE INDEX idx_receipts_task ON receipts(task_id);

CREATE TABLE chain_entries (
  sequence      INTEGER PRIMARY KEY,
  entry_hash    TEXT NOT NULL,
  previous_hash TEXT,
  agent_id      TEXT REFERENCES agents(agent_id),
  agent_name    TEXT, -- name as recorded on the entry itself (may differ from profile)
  entry_type    TEXT,
  timestamp     TEXT,
  public_key    TEXT,
  nonce         TEXT,
  profile_hash  TEXT,
  agent_comment TEXT,
  raw_json      TEXT NOT NULL
);

CREATE TABLE settlements (
  task_id        TEXT PRIMARY KEY REFERENCES tasks(task_id),
  agent_id       TEXT REFERENCES agents(agent_id),
  title          TEXT,
  category       TEXT,
  bounty_usdc    REAL, -- from the settled feed's bounty.amount_display (token: see bounty_token)
  bounty_token   TEXT,
  bounty_network TEXT,
  sponsored      INTEGER, -- 0/1
  created_at     TEXT,
  claimed_at     TEXT,
  submitted_at   TEXT,
  settled_at     TEXT,
  time_to_paid_s INTEGER, -- as computed by the platform in the settled feed
  delivery_s     INTEGER, -- as computed by the platform in the settled feed
  tx_hash        TEXT,
  raw_json       TEXT NOT NULL
);

CREATE TABLE status_events (
  event_id      INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id       TEXT NOT NULL REFERENCES tasks(task_id),
  event_type    TEXT NOT NULL, -- created|claimed|submitted|verified|settled|cancelled|expired|revision_requested|disputed|rated
  event_at      TEXT NOT NULL,
  source_column TEXT NOT NULL  -- the tasks column this event was derived from
);
CREATE INDEX idx_events_task ON status_events(task_id, event_at);

CREATE TABLE meta (
  key   TEXT PRIMARY KEY,
  value TEXT
);
