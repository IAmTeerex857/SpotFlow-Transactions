import { NextResponse, type NextRequest } from "next/server";
import { exchangeCode, idTokenClaims, safeNext, STATE_COOKIE } from "@/lib/auth/google";
import { createClient } from "@/lib/supabase/server";
import { isMemberEmail } from "@/lib/supabase/env";

// Google redirects here (the URI the original site registered). The code is
// exchanged for an ID token, the domain is checked, and Supabase creates the
// session from that token. The database trigger refuses other domains too.
export async function GET(request: NextRequest) {
  const { origin, searchParams } = request.nextUrl;
  const fail = (reason: string) => {
    const response = NextResponse.redirect(`${origin}/login?error=${reason}`);
    response.cookies.delete(STATE_COOKIE);
    return response;
  };

  let saved: { state?: string; next?: string } = {};
  try {
    saved = JSON.parse(request.cookies.get(STATE_COOKIE)?.value ?? "{}");
  } catch {}
  const code = searchParams.get("code");
  if (searchParams.get("error") || !code || !saved.state || saved.state !== searchParams.get("state")) return fail("oauth");

  let idToken: string;
  try {
    idToken = await exchangeCode(code, origin);
  } catch (e) {
    console.error(e);
    return fail("oauth");
  }

  const claims = idTokenClaims(idToken);
  const verified = claims.email_verified === true || claims.email_verified === "true";
  if (!verified || !isMemberEmail(claims.email)) return fail("domain");

  const supabase = await createClient();
  const { error } = await supabase.auth.signInWithIdToken({ provider: "google", token: idToken });
  if (error) {
    console.error("supabase sign-in failed", error.message);
    return fail(/spotflow\.one|database error/i.test(error.message) ? "domain" : "oauth");
  }

  const response = NextResponse.redirect(`${origin}${safeNext(saved.next)}`);
  response.cookies.delete(STATE_COOKIE);
  return response;
}
