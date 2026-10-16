import assert from "node:assert/strict";
import { mkdtemp, mkdir, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { browserSchemaSql, postgresImageFromCompose, redactSmokeSecrets, runContainerStartupSmoke } from "../container-startup-smoke.mjs";

const pinnedPostgres = `pgvector/pgvector:test@sha256:${"a".repeat(64)}`;

/** Makes a disposable source fixture and a Docker command double; no Docker daemon is used. */
async function smokeFixture(t, { arch = "amd64", failStartup = false, foreignOwnership = false } = {}) {
  const root = await mkdtemp(join(tmpdir(), "spaceapp-smoke-test-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  await mkdir(join(root, "packages/run-spaceapp/templates"), { recursive: true });
  await mkdir(join(root, "packages/db/migrations"), { recursive: true });
  await writeFile(join(root, "packages/run-spaceapp/templates/compose.yml"), `services:\n  postgres:\n    image: ${pinnedPostgres}\nnetworks:\n`);
  await writeFile(join(root, "packages/db/migrations/0001_first.sql"), "SELECT 1;");
  await writeFile(join(root, "packages/db/migrations/0002_second.sql"), "SELECT 2;");
  const calls = [], resources = new Map();
  const docker = async (args) => {
    calls.push([...args]);
    if (args[0] === "image") return arch;
    if (args[0] === "network" && args[1] === "create") {
      resources.set(args.at(-1), args[args.indexOf("--label") + 1].split("=")[1]); return "created";
    }
    if (args[0] === "run") {
      resources.set(args[args.indexOf("--name") + 1], args[args.indexOf("--label") + 1].split("=")[1]); return "started";
    }
    if (args[0] === "inspect" && args.includes("{{.State.Running}}")) return failStartup ? "false" : "true";
    if (args[0] === "inspect" || (args[0] === "network" && args[1] === "inspect")) {
      return foreignOwnership && args[0] === "inspect" ? "another-owner" : resources.get(args.at(-1));
    }
    if (args[0] === "rm" || (args[0] === "network" && args[1] === "rm")) {
      resources.delete(args.at(-1)); return "removed";
    }
    if (args[0] === "exec" && args.includes("node") && failStartup) throw new Error("synthetic startup failure");
    if (args.includes("-Atc")) return "2";
    if (args.includes("/usr/bin/chromium")) return "<html><title>spaceapp-startup-smoke</title></html>";
    return "ok";
  };
  return { root, docker, calls, resources };
}

test("startup smoke uses the Compose pgvector digest and rejects an unpinned database", () => {
  assert.equal(postgresImageFromCompose(`services:\n  postgres:\n    image: ${pinnedPostgres}\nnetworks:\n`), pinnedPostgres);
  assert.throws(() => postgresImageFromCompose("services:\n  postgres:\n    image: postgres:latest\nnetworks:\n"), /digest-pinned pgvector/);
});

test("browser schema preserves full-filename migration order and exact identities", () => {
  const sql = browserSchemaSql([
    { filename: "0034_second.sql", sql: "SELECT 2;" },
    { filename: "0034_first.sql", sql: "SELECT 1;" }
  ]);
  assert.ok(sql.indexOf("SELECT 1;") < sql.indexOf("SELECT 2;"));
  assert.match(sql, /VALUES \('0034_first', '0034_first.sql'\)/);
  assert.match(sql, /VALUES \('0034_second', '0034_second.sql'\)/);
  assert.equal(sql.match(/BEGIN;/g).length, 2);
  assert.throws(() => browserSchemaSql([{ filename: "../bad.sql", sql: "" }]), /Invalid source migration/);
});

test("diagnostics redact generated secrets including their occurrence inside a connection URL", () => {
  const password = "synthetic-private-password";
  const url = `postgresql://spaceapp:${password}@postgres/spaceapp`;
  const output = redactSmokeSecrets(`failed ${url}; password=${password}`, [password, url]);
  assert.equal(output, "failed [REDACTED]; password=[REDACTED]");
});

for (const target of ["core", "browser", "cli"]) {
  test(`${target} orchestration invokes its real entrypoint/probes and cleans only labelled resources`, async (t) => {
    const fixture = await smokeFixture(t);
    const result = await runContainerStartupSmoke({ target, image: "spaceapp-test:image", arch: "amd64", root: fixture.root }, { docker: fixture.docker });
    assert.equal(result.passed, true); assert.equal(result.cleaned, true);
    assert.equal(result.fullStackLargeProfileTest, false);
    const serviceRun = fixture.calls.find((args) => args[0] === "run" && args.at(-1) === "spaceapp-test:image");
    assert.ok(serviceRun); assert.ok(!serviceRun.includes("--entrypoint"));
    assert.ok(serviceRun.includes("SPACEAPP_WORKFLOWS_ENABLED=false"));
    assert.equal(fixture.resources.size, 0);
    assert.ok(fixture.calls.filter((args) => args[0] === "rm").every((args) => args.includes("-v") && args.at(-1).startsWith("spaceapp-smoke-")));
    if (target === "core") assert.ok(fixture.calls.some((args) => args.includes("SELECT count(*) FROM space_schema_migrations")));
    if (target === "browser") {
      assert.ok(fixture.calls.some((args) => args[0] === "cp" && args.at(-1).endsWith(":/tmp/spaceapp-smoke-schema.sql")));
      assert.ok(fixture.calls.some((args) => args.includes("/usr/bin/chromium") && args.includes("10001:10001")));
      assert.ok(fixture.calls.some((args) => args.includes("--input-type=module")));
    }
    if (target === "cli") assert.equal(fixture.calls.filter((args) => args[0] === "run").length, 1);
    const mountedFile = serviceRun.find((arg) => arg.startsWith("type=bind,src="));
    const temporaryDirectory = mountedFile.match(/src=(.*)\/database-url,dst=/)[1];
    await assert.rejects(readdir(temporaryDirectory), { code: "ENOENT" });
  });
}

test("failed startup still attempts cleanup and preserves the failure", async (t) => {
  const fixture = await smokeFixture(t, { failStartup: true });
  await assert.rejects(runContainerStartupSmoke({ target: "cli", image: "spaceapp-test:image", arch: "amd64", root: fixture.root }, { docker: fixture.docker }), /exited before becoming ready/);
  assert.equal(fixture.resources.size, 0);
  assert.ok(fixture.calls.some((args) => args[0] === "network" && args[1] === "rm"));
});

test("cleanup refuses to delete a container whose ownership label does not match", async (t) => {
  const fixture = await smokeFixture(t, { foreignOwnership: true });
  await assert.rejects(runContainerStartupSmoke({ target: "cli", image: "spaceapp-test:image", arch: "amd64", root: fixture.root }, { docker: fixture.docker }), /Refusing to remove container/);
  assert.equal(fixture.calls.filter((args) => args[0] === "rm").length, 0);
});

test("wrong image architecture fails before creating any Docker resources", async (t) => {
  const fixture = await smokeFixture(t, { arch: "amd64" });
  await assert.rejects(runContainerStartupSmoke({ target: "cli", image: "spaceapp-test:image", arch: "arm64", root: fixture.root }, { docker: fixture.docker }), /Expected arm64/);
  assert.equal(fixture.calls.filter((args) => args[0] === "run" || args[0] === "network").length, 0);
});

test("a network created before its Docker command fails is still cleaned by ownership label", async (t) => {
  const fixture = await smokeFixture(t);
  const docker = async (args, options) => {
    const result = await fixture.docker(args, options);
    if (args[0] === "network" && args[1] === "create") throw new Error("synthetic interrupted network create");
    return result;
  };
  await assert.rejects(runContainerStartupSmoke({ target: "cli", image: "spaceapp-test:image", arch: "amd64", root: fixture.root }, { docker }), /interrupted network create/);
  assert.equal(fixture.resources.size, 0);
  assert.ok(fixture.calls.some((args) => args[0] === "network" && args[1] === "rm"));
});
