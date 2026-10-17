import "server-only";
import { redirect } from "next/navigation";
import { createClient } from "@/lib/supabase/server";
import { isMemberEmail } from "@/lib/supabase/env";

interface Member {
  id: string;
  email: string;
  name: string | null;
  avatarUrl: string | null;
}

/** Verifies the session JWT and the Spotflow domain. Returns null otherwise. */
export async function getMember() {
  const supabase = await createClient();
  const { data, error } = await supabase.auth.getClaims();
  const claims = data?.claims;
  if (error || !claims || !isMemberEmail(claims.email as string | undefined)) return { supabase, member: null };
  const meta = (claims.user_metadata ?? {}) as Record<string, unknown>;
  const member: Member = {
    id: claims.sub,
    email: claims.email as string,
    name: (meta.full_name as string) ?? (meta.name as string) ?? null,
    avatarUrl: (meta.avatar_url as string) ?? (meta.picture as string) ?? null,
  };
  return { supabase, member };
}

/** For pages: redirects to the sign-in page when there is no member session. */
export async function requireMember() {
  const { supabase, member } = await getMember();
  if (!member) redirect("/login");
  return { supabase, member };
}
