import { NextResponse } from "next/server";
import { getMember } from "@/lib/server/auth";

/** Deletes an upload and its transactions. Row level security limits this to the uploader. */
export async function DELETE(_request: Request, { params }: RouteContext<"/api/uploads/[id]">) {
  const { supabase, member } = await getMember();
  if (!member) return NextResponse.json({ error: "Unauthorised" }, { status: 401 });
  const { id } = await params;
  const { data, error } = await supabase.from("uploads").delete().eq("id", id).select("id, storage_path");
  if (error) return NextResponse.json({ error: error.message }, { status: 500 });
  if (!data?.length) return NextResponse.json({ error: "Only the person who uploaded an export can delete it" }, { status: 403 });
  if (data[0].storage_path) await supabase.storage.from("exports").remove([data[0].storage_path]);
  return NextResponse.json({ deleted: id });
}
