// node --conditions=react-server --import tsx scripts/check-db-recovery.mts
// Two targets:
// - LOCAL_TEST_DATABASE_URL: a local PostgreSQL;
// - RECOVERY_SUPABASE=1 with the app's read-only PG_* variables on port 5432.
//   Read-only (SELECT 1, pg_sleep, pg_backend_pid); it terminates only the
//   backend of its own connection, which session mode (5432) dedicates to it.
//   Never 6543: there a backend pid can be another client's transaction.
import assert from "node:assert/strict";
import { Pool, type PoolConfig } from "pg";
// tsx compiles src/ as CommonJS, whose class exports Node cannot name.
import dbModule from "../src/server/db";
const { SnapshotDatabase, Deadline, BusyError } = dbModule;

function target(): PoolConfig {
  if (process.env.RECOVERY_SUPABASE === "1") {
    if (process.env.PG_PORT !== "5432")
      throw new Error("Supabase recovery checks require PG_PORT=5432 (session mode)");
    if (!process.env.PG_HOST || !process.env.PG_USER)
      throw new Error("Set the app's read-only PG_* variables");
    return {
      host: process.env.PG_HOST,
      port: 5432,
      database: process.env.PG_DB,
      user: process.env.PG_USER,
      password: process.env.PG_PASS,
      ssl: { rejectUnauthorized: false },
    };
  }
  const raw = process.env.LOCAL_TEST_DATABASE_URL;
  if (!raw) throw new Error("Set LOCAL_TEST_DATABASE_URL, or RECOVERY_SUPABASE=1 with PG_*");
  if (!["localhost", "127.0.0.1", "[::1]", "::1"].includes(new URL(raw).hostname))
    throw new Error("LOCAL_TEST_DATABASE_URL must be a local database");
  return { connectionString: raw };
}
const config = target();
const name = `ibpl-recovery-${process.pid}-${Date.now()}`;
const pool = new Pool({ ...config, application_name: name, max: 1, connectionTimeoutMillis: 0 });
const observer = new Pool({ ...config, application_name: `${name}-observer`, max: 1 });
const idleErrors: string[] = [];
pool.on("error", (error) => idleErrors.push(error.name));
const db = new SnapshotDatabase(pool, 1, 2);
const success = () => db.withSnapshot(new Deadline(), (q) => q<{ value: number }>("SELECT 1::int AS value", []));
const rejected = (work: Promise<unknown>) => work.then(() => { throw Error("Expected request failure"); }, (e: unknown) => e);
async function until(check: () => boolean, label: string) {
  const end = Date.now() + 3000;
  while (!check()) {
    if (Date.now() > end) throw Error(`${label} did not return to baseline`);
    await new Promise((yes) => setTimeout(yes, 20));
  }
}
const checks: string[] = [];
try {
  assert.equal((await success())[0].value, 1);
  const sqlFailure = await rejected(db.withSnapshot(new Deadline(1500, 100), (q) => q("SELECT pg_sleep(5)", [])));
  assert.equal((sqlFailure as { code: string }).code, "57014");
  await until(() => pool.totalCount === 0 && pool.waitingCount === 0, "statement timeout");
  assert.equal((await success())[0].value, 1);
  checks.push("statement timeout destroys the real connection and recovers");

  let entered!: (pid: number) => void;
  const ready = new Promise<number>((yes) => { entered = yes; });
  const killed = rejected(db.withSnapshot(new Deadline(8000, 7000), async (q) => {
    entered((await q<{ pid: number }>("SELECT pg_backend_pid() AS pid", []))[0].pid);
    await q("SELECT pg_sleep(5)", []);
  }));
  const pid = await ready;
  await observer.query("SELECT pg_terminate_backend($1::int)", [pid]);
  assert.ok(await killed instanceof Error);
  await until(() => pool.totalCount === 0 && pool.waitingCount === 0, "terminated connection");
  assert.equal((await success())[0].value, 1);
  checks.push("terminated checked-out connection recovers without a stranded checkout");

  const held = await pool.connect();
  try {
    let called = false;
    const late = await rejected(db.withSnapshot(new Deadline(3000), async () => { called = true; }));
    assert.ok(late instanceof BusyError);
    assert.equal(called, false);
  } finally { held.release(); }
  await until(() => pool.totalCount === 0 && pool.waitingCount === 0, "late checkout");
  assert.equal((await success())[0].value, 1);
  checks.push("late real-pool checkout is destroyed after wrapper acquisition timeout");

  for (let i = 0; i < 20; i++) {
    await rejected(db.withSnapshot(new Deadline(), async () => { throw Error("callback failure"); }));
    assert.equal((await success())[0].value, 1);
    assert.equal(pool.waitingCount, 0);
    assert.equal(pool.totalCount, 1);
    assert.equal(pool.idleCount, 1);
  }
  checks.push("20 failure/success cycles leave one idle connection and no waiters");
  await pool.end();
  const live = await observer.query<{ count: string }>("SELECT COUNT(*)::text AS count FROM pg_stat_activity WHERE application_name=$1", [name]);
  assert.equal(Number(live.rows[0].count), 0);
  console.log(JSON.stringify({ passed: checks.length, checks, remainingConnections: 0, idleErrors }, null, 2));
} finally {
  if (!pool.ended) await pool.end();
  await observer.end();
}
