import { NextResponse, type NextRequest } from "next/server";
import { z } from "zod";
import { getMember } from "@/lib/server/auth";

const Body = z.object({
  kind: z.enum(["provider", "region"]),
  name: z.string().trim().min(2).max(64).regex(/^[\p{L}\p{N} _'.-]+$/u, "Unexpected characters"),
});

/** Adds a provider or region so future (and reprocessed) uploads recognise it. */
export async function POST(request: NextRequest) {
  const { supabase, member } = await getMember();
  if (!member) return NextResponse.json({ error: "Unauthorised" }, { status: 401 });
  const parsed = Body.safeParse(await request.json().catch(() => null));
  if (!parsed.success) return NextResponse.json({ error: parsed.error.issues[0]?.message ?? "Invalid input" }, { status: 400 });
  const name = parsed.data.kind === "provider" ? parsed.data.name.toLowerCase() : parsed.data.name;
  const { error } = await supabase.from("registry_entries").upsert({ kind: parsed.data.kind, name }, { onConflict: "kind,name", ignoreDuplicates: true });
  if (error) return NextResponse.json({ error: error.message }, { status: 500 });
  return NextResponse.json({ kind: parsed.data.kind, name });
}
