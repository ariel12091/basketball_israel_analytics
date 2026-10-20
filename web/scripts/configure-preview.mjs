// Copies only the existing read-only connection settings to the linked preview.
// Secret values travel on stdin, never command arguments or logs.
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { execFileSync } from "node:child_process";
import path from "node:path";
process.loadEnvFile(
  fileURLToPath(new URL("../../app/.Renviron", import.meta.url)),
);
const command = execFileSync("where.exe", ["vercel.cmd"], { encoding: "utf8" })
  .trim()
  .split(/\r?\n/)[0];
const cli = path.join(
  path.dirname(command),
  "node_modules/vercel/dist/index.js",
);
if (!process.env.PG_USER?.startsWith("app_readonly"))
  throw new Error("Expected existing app_readonly login");
for (const key of [
  "PG_HOST",
  "PG_PORT",
  "PG_DB",
  "PG_USER",
  "PG_PASS",
  "PG_SSLMODE",
]) {
  const value =
    process.env[key] ??
    (key === "PG_SSLMODE" ? "require" : key === "PG_PORT" ? "6543" : "");
  if (!value) throw new Error("Missing " + key);
  await new Promise((resolve, reject) => {
    const child = spawn(
      process.execPath,
      [
        cli,
        "env",
        "add",
        key,
        "--environment",
        "preview",
        "--sensitive",
        "--yes",
      ],
      { stdio: ["pipe", "pipe", "pipe"] },
    );
    child.stdout.resume();
    child.stderr.resume();
    child.on("error", reject);
    child.on("exit", (code) =>
      code === 0 ? resolve() : reject(new Error("Failed to configure " + key)),
    );
    child.stdin.end(value);
  });
  console.log("Configured preview " + key);
}
