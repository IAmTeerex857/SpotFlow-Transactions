import { NextResponse, type NextRequest } from "next/server";
import { getMember } from "@/lib/server/auth";

/** Has this exact file been uploaded before? Used to warn before saving. */
export async function GET(request: NextRequest) {
  const { supabase, member } = await getMember();
  if (!member) return NextResponse.json({ error: "Unauthorised" }, { status: 401 });
  const sha = request.nextUrl.searchParams.get("sha") ?? "";
  if (!/^[0-9a-f]{64}$/.test(sha)) return NextResponse.json({ error: "Invalid hash" }, { status: 400 });
  const { data } = await supabase
    .from("uploads")
    .select("id, created_at, uploader_email, file_name")
    .eq("file_sha256", sha)
    .eq("status", "complete")
    .order("created_at", { ascending: false })
    .limit(1);
  return NextResponse.json({ previous: data?.[0] ?? null });
}
