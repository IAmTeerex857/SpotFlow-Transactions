// Runs the real migration in PGlite (Postgres compiled to WASM) against a
// stub of Supabase's auth schema, then checks that the SQL report matches the
// TypeScript report for the same rows, that re-uploads are idempotent, and
// that row level security and the domain trigger hold.
import { readFileSync, existsSync, readdirSync } from "node:fs";
import path from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { beforeAll, describe, expect, it } from "vitest";
import { createPseudonymizer, toIngestRows, chunk, type IngestRow } from "@/lib/ingest/prepare";
import { parseExport } from "@/lib/parser/parse";
import { buildReport } from "@/lib/report/build";
import { cubeFromTransactions } from "@/lib/report/cube";
import type { Cube } from "@/lib/report/types";

const MIGRATIONS = path.join(__dirname, "migrations");
const SAMPLE = path.resolve(__dirname, "../../data/october/Oct 15 - export anonymized.csv");

const SYNTHETIC = [
  "Merchant,Amount,Currency,Rate,Provider,Region,Customer,Payment Status,Payment Channel,Provider Message,Payment Date",
  "Acme,5,USD,1435.53,paystack,Nigeria,a@x.com,failed,card,Insufficient funds,2026-10-15T10:00:00Z",
  "Acme,5,USD,1435.53,paystack,Nigeria,a@x.com,successful,card,success,2026-10-15T10:05:00Z",
  "Acme,51.21,GHS,12.63,hubtel,Ghana,b@x.com,failed,mobile_money,Token abc is locked,2026-10-15T11:00:00Z",
  "Acme,51.21,GHS,12.63,hubtel,Ghana,b@x.com,abandoned,mobile_money,,2026-10-15T09:00:00Z",
  "Beta,10,USD,1,pawapay,Zambia,c@x.com,successful,mobile_money,success,2026-10-15T12:00:00Z",
].join("\n");

const AUTH_STUB = `
  create role anon nologin; create role authenticated nologin; create role service_role nologin;
  create schema auth;
  create table auth.users (id uuid primary key default gen_random_uuid(), email text);
  create function auth.jwt() returns jsonb language sql stable as
    $$ select coalesce(nullif(current_setting('request.jwt.claims', true), ''), '{}')::jsonb $$;
  create function auth.uid() returns uuid language sql stable as
    $$ select nullif(auth.jwt() ->> 'sub', '')::uuid $$;
  grant usage on schema public, auth to authenticated, anon;
`;

let db: PGlite;
let rows: IngestRow[];
let uploadId: string;

async function ingest(id: string, data: IngestRow[]) {
  let inserted = 0;
  let updated = 0;
  for (const part of chunk(data, 500)) {
    const res = await db.query<{ r: { inserted: number; updated: number } }>(
      "select public.ingest_transactions($1, $2::jsonb) as r", [id, JSON.stringify(part)],
    );
    inserted += res.rows[0].r.inserted;
    updated += res.rows[0].r.updated;
  }
  return { inserted, updated };
}

async function asUser(email: string, sub: string, fn: () => Promise<void>) {
  await db.exec(`set role authenticated; select set_config('request.jwt.claims', '${JSON.stringify({ email, sub })}', false);`);
  try {
    await fn();
  } finally {
    await db.exec("reset role; select set_config('request.jwt.claims', '', false);");
  }
}

beforeAll(async () => {
  db = new PGlite();
  await db.exec(AUTH_STUB);
  for (const file of readdirSync(MIGRATIONS).sort()) {
    await db.exec(readFileSync(path.join(MIGRATIONS, file), "utf8"));
  }
  await db.exec("grant select, insert, update, delete on all tables in schema public to authenticated;");

  const text = existsSync(SAMPLE) ? readFileSync(SAMPLE, "utf8") : SYNTHETIC;
  const parsed = parseExport(text);
  rows = toIngestRows(parsed, {
    pseudonymize: createPseudonymizer("test-secret-test-secret-test-secret!"),
    fallbackDate: "2026-10-15",
  }).rows;

  const up = await db.query<{ id: string }>(
    "insert into public.uploads (file_name, file_sha256, parser_version) values ('t.csv', 'x', 't') returning id",
  );
  uploadId = up.rows[0].id;
}, 60_000);

describe("database", () => {
  it("ingests every row once and re-uploads idempotently", async () => {
    const first = await ingest(uploadId, rows);
    expect(first).toEqual({ inserted: rows.length, updated: 0 });
    const again = await ingest(uploadId, rows);
    expect(again).toEqual({ inserted: 0, updated: rows.length });
    const count = await db.query<{ n: number }>("select count(*)::int as n from public.transactions");
    expect(count.rows[0].n).toBe(rows.length);
  });

  it("produces the same report as the TypeScript cube", async () => {
    const dates = rows.map((r) => r.business_date).sort();
    const res = await db.query<{ cube: Cube }>("select public.report_cube($1, $2) as cube", [dates[0], dates[dates.length - 1]]);
    const sqlReport = buildReport(res.rows[0].cube);

    const tsReport = buildReport(cubeFromTransactions(rows.map((r, i) => ({
      merchant: r.merchant, provider: r.provider, region: r.region, status: r.status, channel: r.channel,
      currency: r.currency, amountUsd: r.amount_usd, message: r.message, paidAt: r.paid_at,
      customerId: r.customer_id ?? "", businessDate: r.business_date, seq: i,
    }))));

    const comparable = (r: ReturnType<typeof buildReport>) => JSON.parse(JSON.stringify(r, (_k, v) =>
      typeof v === "number" ? Math.round(v * 1e6) / 1e6 : v));
    const sql = comparable(sqlReport);
    const ts = comparable(tsReport);
    expect(sql.totals).toEqual(ts.totals);
    expect(sql.daily).toEqual(ts.daily);
    expect(sql.merchants).toEqual(ts.merchants);
    expect(sql.providers).toEqual(ts.providers);
    expect(sql.regions).toEqual(ts.regions);
    expect(sql.peaks).toEqual(ts.peaks);
    expect(sql.retry).toEqual(ts.retry);
    expect(sqlReport.totals.total).toBe(rows.length);
  });

  it("hides every row from non-members", async () => {
    await asUser("someone@gmail.com", crypto.randomUUID(), async () => {
      const res = await db.query<{ n: number }>("select count(*)::int as n from public.transactions");
      expect(res.rows[0].n).toBe(0);
    });
    await asUser("someone@evil.spotflow.one", crypto.randomUUID(), async () => {
      const res = await db.query<{ n: number }>("select count(*)::int as n from public.transactions");
      expect(res.rows[0].n).toBe(0);
    });
    await asUser("analyst@spotflow.one", crypto.randomUUID(), async () => {
      const res = await db.query<{ n: number }>("select count(*)::int as n from public.transactions");
      expect(res.rows[0].n).toBe(rows.length);
    });
  });

  it("lets members create uploads only as themselves", async () => {
    const sub = crypto.randomUUID();
    await db.query("insert into auth.users (id, email) values ($1, 'analyst@spotflow.one')", [sub]);
    await asUser("analyst@spotflow.one", sub, async () => {
      const ok = await db.query<{ uploader_email: string }>(
        "insert into public.uploads (file_name, file_sha256, parser_version) values ('a.csv', 'y', 't') returning uploader_email",
      );
      expect(ok.rows[0].uploader_email).toBe("analyst@spotflow.one");
      await expect(db.query(
        "insert into public.uploads (file_name, file_sha256, parser_version, uploaded_by) values ('b.csv', 'z', 't', gen_random_uuid())",
      )).rejects.toThrow();
    });
  });

  it("rejects sign-ups outside spotflow.one", async () => {
    await expect(db.query("insert into auth.users (email) values ('person@gmail.com')")).rejects.toThrow(/spotflow\.one/);
    await expect(db.query("insert into auth.users (email) values ('person@spotflow.one.evil.com')")).rejects.toThrow();
    await db.query("insert into auth.users (email) values ('Person@Spotflow.One')");
  });
});
