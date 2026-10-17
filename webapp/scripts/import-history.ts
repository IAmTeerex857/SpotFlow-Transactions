// One-time import of the historical anonymised exports into Supabase.
//
//   npm run import:history -- --dir "../data" [--dry-run] [--limit 5]
//
// Needs NEXT_PUBLIC_SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY and
// PSEUDONYM_SECRET (in .env.local). Files already imported (same content
// hash) are skipped, so the script can be re-run safely.
import { globSync, readFileSync } from "node:fs";
import path from "node:path";
import { sha256 } from "@/lib/ingest/prepare";
import { stripBom } from "@/lib/parser/csv";
import { parseExport } from "@/lib/parser/parse";
import { ingestExport } from "@/lib/server/ingest";
import { createAdminClient } from "@/lib/supabase/admin";

function arg(name: string): string | undefined {
  const i = process.argv.indexOf(`--${name}`);
  return i === -1 ? undefined : process.argv[i + 1];
}

async function main() {
  const dir = path.resolve(arg("dir") ?? path.join(process.cwd(), "..", "data"));
  const dryRun = process.argv.includes("--dry-run");
  const limit = Number(arg("limit") ?? Infinity);
  const files = globSync(path.join(dir, "**/*anonymized.csv")).sort();
  console.log(`Found ${files.length} exports under ${dir}${dryRun ? " (dry run)" : ""}`);

  const supabase = dryRun ? null : createAdminClient();
  const existing = new Set<string>();
  if (supabase) {
    const { data, error } = await supabase.from("uploads").select("file_sha256").eq("status", "complete");
    if (error) throw new Error(`Could not read uploads: ${error.message}. Has the migration been applied?`);
    for (const row of data) existing.add(row.file_sha256);
  }

  let done = 0;
  let skipped = 0;
  let transactions = 0;
  for (const file of files.slice(0, limit)) {
    const text = readFileSync(file, "utf8");
    const name = path.relative(dir, file);
    if (existing.has(sha256(stripBom(text)))) {
      skipped += 1;
      continue;
    }
    if (dryRun || !supabase) {
      const { stats } = parseExport(text);
      transactions += stats.transactions;
      done += 1;
      console.log(`  ${name}: ${stats.transactions} transactions (${stats.businessDate ?? "no date"})`);
      continue;
    }
    const result = await ingestExport(supabase, {
      text,
      fileName: path.basename(file),
      bytes: Buffer.byteLength(text),
      userId: null,
      source: "legacy_import",
    });
    done += 1;
    transactions += result.stats.transactions;
    console.log(`  ✓ ${name}: ${result.inserted} new, ${result.updated} updated`);
  }
  console.log(`\n${dryRun ? "Would import" : "Imported"} ${done} files, ${transactions.toLocaleString()} transactions; skipped ${skipped} already imported.`);
}

main().catch((e) => {
  console.error(e instanceof Error ? e.message : e);
  process.exit(1);
});
