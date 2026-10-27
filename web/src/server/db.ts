import "server-only";
import { Pool, types, type PoolClient } from "pg";
import { readFileSync } from "node:fs";
// RPostgres uses bigint="numeric". Counts in these basketball frames are
// within the exact integer range; dates remain date-only strings in every TZ.
for (const oid of [20, 1700])
  types.setTypeParser(oid, (value: string) => {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || Math.abs(parsed) > Number.MAX_SAFE_INTEGER)
      throw new Error("Database number outside safe range");
    return parsed;
  });
types.setTypeParser(1082, (value: string) => value);
import {
  Deadline,
  DeadlineError,
  BusyError,
  QUEUE_MS,
  CONNECT_MS,
} from "./deadline";
export { Deadline, DeadlineError, BusyError } from "./deadline";
export type Query = <R = Record<string, unknown>>(
  sql: string,
  params: unknown[],
) => Promise<R[]>;
type Waiting = {
  resolve: () => void;
  reject: (e: Error) => void;
  deadline: Deadline;
  timer: ReturnType<typeof setTimeout>;
};
function validateLimits(max: number, queueMax: number) {
  if (!Number.isInteger(max) || max < 1 || max > 20)
    throw new Error("Invalid DB_POOL_MAX");
  if (!Number.isInteger(queueMax) || queueMax < 0 || queueMax > 1000)
    throw new Error("Invalid DB_QUEUE_MAX");
}
export function snapshotLimits(
  env: Record<string, string | undefined> = process.env,
) {
  const max = Number(env.DB_POOL_MAX ?? 2),
    queueMax = Number(env.DB_QUEUE_MAX ?? 20);
  if (env.DB_POOL_MAX?.trim() === "" || env.DB_QUEUE_MAX?.trim() === "")
    throw new Error("Empty database limit");
  validateLimits(max, queueMax);
  return { max, queueMax };
}
/** FIFO bounds admitted work before pg's internal queue, so expired work cannot execute later. */
export class SnapshotDatabase {
  private active = 0;
  private waiting: Waiting[] = [];
  constructor(
    private pool: Pick<Pool, "connect">,
    private max = 2,
    private queueMax = 20,
  ) {
    validateLimits(max, queueMax);
  }
  private async enter(deadline: Deadline) {
    if (deadline.expired()) throw new DeadlineError();
    if (this.active < this.max) {
      this.active++;
      return;
    }
    if (this.waiting.length >= this.queueMax) throw new BusyError();
    await new Promise<void>((resolve, reject) => {
      const entry = {} as Waiting;
      Object.assign(entry, {
        resolve,
        reject,
        deadline,
        timer: setTimeout(
          () => {
            this.waiting = this.waiting.filter((e) => e !== entry);
            reject(deadline.expired() ? new DeadlineError() : new BusyError());
          },
          Math.min(QUEUE_MS, deadline.remaining()),
        ),
      });
      this.waiting.push(entry);
    });
  }
  private leave() {
    this.active--;
    while (this.waiting.length) {
      const next = this.waiting.shift()!;
      clearTimeout(next.timer);
      if (next.deadline.expired()) {
        next.reject(new DeadlineError());
        continue;
      }
      this.active++;
      next.resolve();
      break;
    }
  }
  async withSnapshot<T>(
    deadline: Deadline,
    fn: (q: Query) => Promise<T>,
  ): Promise<T> {
    await this.enter(deadline);
    let client: PoolClient | undefined,
      released = false,
      stopped = false;
    let overall: ReturnType<typeof setTimeout> | undefined,
      first: ReturnType<typeof setTimeout> | undefined;
    const destroy = () => {
      if (client && !released) {
        released = true;
        client.release(true);
      }
    };
    try {
      return await new Promise<T>((resolve, reject) => {
        const fail = (e: unknown) => {
          if (stopped) return;
          stopped = true;
          destroy();
          reject(e);
        };
        overall = setTimeout(
          () => fail(new DeadlineError()),
          deadline.remaining(),
        );
        first = setTimeout(
          () => fail(new BusyError()),
          Math.min(CONNECT_MS, deadline.remaining()),
        );
        void (async () => {
          try {
            const checked = await this.pool.connect();
            client = checked;
            if (stopped) {
              destroy();
              return;
            }
            // The timeouts ride the BEGIN round trip (a computed integer, not
            // a user value): every statement may use the request's SQL budget,
            // the total is checked before each one, and the overall deadline
            // destroys the connection whatever is running.
            const budget = Math.floor(
              Math.min(deadline.sqlMaxMs, deadline.remaining() - 500),
            );
            if (budget <= 0) throw new DeadlineError();
            await client.query(
              `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET LOCAL statement_timeout = '${budget}ms'; SET LOCAL idle_in_transaction_session_timeout = '5s'`,
            );
            if (stopped) return;
            clearTimeout(first);
            let sqlTime = 0;
            const q: Query = async <R>(sql: string, params: unknown[]) => {
              if (stopped || deadline.expired()) throw new DeadlineError();
              if (sqlTime >= deadline.sqlMaxMs || deadline.remaining() <= 500)
                throw new DeadlineError();
              const began = performance.now();
              try {
                const answer = await client!.query(sql, params);
                if (stopped) throw new DeadlineError();
                return answer.rows as R[];
              } finally {
                sqlTime += performance.now() - began;
              }
            };
            const result = await fn(q);
            if (stopped || deadline.expired()) throw new DeadlineError();
            await client.query("COMMIT");
            if (stopped) return;
            stopped = true;
            released = true;
            client.release();
            resolve(result);
          } catch (e) {
            fail(e);
          }
        })();
      });
    } finally {
      clearTimeout(overall);
      clearTimeout(first);
      this.leave();
    }
  }
}
let database: SnapshotDatabase | undefined;
export function withSnapshot<T>(
  deadline: Deadline,
  fn: (q: Query) => Promise<T>,
): Promise<T> {
  if (!database) {
    const connectionString =
      process.env.DATABASE_URL_WEB || process.env.DATABASE_URL;
    if (!connectionString && !process.env.PG_HOST) throw new BusyError();
    const { max, queueMax } = snapshotLimits();
    const pool = new Pool({
      ...(connectionString
        ? { connectionString }
        : {
            host: process.env.PG_HOST,
            port: Number(process.env.PG_PORT ?? 5432),
            database: process.env.PG_DB,
            user: process.env.PG_USER,
            password: process.env.PG_PASS,
          }),
      // Match Shiny's sslmode=require. Operators can supply their project's
      // root CA and PG_SSLMODE=verify-full for certificate verification.
      ssl:
        process.env.PG_SSLMODE === "disable"
          ? false
          : {
              rejectUnauthorized: process.env.PG_SSLMODE === "verify-full",
              ...(process.env.PG_SSLROOTCERT
                ? { ca: readFileSync(process.env.PG_SSLROOTCERT, "utf8") }
                : {}),
            },
      max,
      connectionTimeoutMillis: CONNECT_MS,
      idleTimeoutMillis: 10000,
      application_name: "ibpl-web",
    });
    pool.on("error", (error: Error & { code?: string }) => {
      console.error("Database idle client failed", error.code ?? error.name);
    });
    database = new SnapshotDatabase(pool, max, queueMax);
  }
  return database.withSnapshot(deadline, fn);
}
