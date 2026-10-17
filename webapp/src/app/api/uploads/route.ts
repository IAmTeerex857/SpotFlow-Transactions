import { gunzipSync } from "node:zlib";
import { NextResponse, type NextRequest } from "next/server";
import { getMember } from "@/lib/server/auth";
import { IngestError, ingestExport, MAX_EXPORT_BYTES } from "@/lib/server/ingest";

export const maxDuration = 120;

// The browser gzips the CSV before sending, which keeps month-sized exports
// under the platform's request body limit.
export async function POST(request: NextRequest) {
  const { supabase, member } = await getMember();
  if (!member) return NextResponse.json({ error: "Sign in with a spotflow.one account" }, { status: 401 });

  const fileName = decodeURIComponent(request.headers.get("x-file-name") ?? "export.csv");
  if (!/\.csv$/i.test(fileName)) return NextResponse.json({ error: "Upload a .csv export" }, { status: 400 });

  let text: string;
  let bytes: number;
  try {
    const body = Buffer.from(await request.arrayBuffer());
    const raw = request.headers.get("content-type") === "application/gzip"
      ? gunzipSync(body, { maxOutputLength: MAX_EXPORT_BYTES })
      : body;
    bytes = raw.byteLength;
    if (bytes > MAX_EXPORT_BYTES) throw new Error("too large");
    text = raw.toString("utf8");
  } catch {
    return NextResponse.json({ error: "The file is unreadable or larger than 60 MB" }, { status: 413 });
  }

  try {
    const result = await ingestExport(supabase, { text, fileName, bytes, userId: member.id });
    return NextResponse.json(result);
  } catch (e) {
    if (e instanceof IngestError) return NextResponse.json({ error: e.message, detail: e.detail }, { status: e.status });
    console.error("upload failed", e);
    return NextResponse.json({ error: "Something went wrong while saving the export" }, { status: 500 });
  }
}
