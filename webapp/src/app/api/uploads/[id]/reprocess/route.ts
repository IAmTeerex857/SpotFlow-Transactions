import { NextResponse } from "next/server";
import { getMember } from "@/lib/server/auth";
import { IngestError, reprocessUpload } from "@/lib/server/ingest";

export const maxDuration = 120;

export async function POST(_request: Request, { params }: RouteContext<"/api/uploads/[id]/reprocess">) {
  const { supabase, member } = await getMember();
  if (!member) return NextResponse.json({ error: "Unauthorised" }, { status: 401 });
  const { id } = await params;
  try {
    return NextResponse.json(await reprocessUpload(supabase, id));
  } catch (e) {
    if (e instanceof IngestError) return NextResponse.json({ error: e.message }, { status: e.status });
    console.error("reprocess failed", e);
    return NextResponse.json({ error: "Reprocessing failed" }, { status: 500 });
  }
}
