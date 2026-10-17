import { NextResponse, type NextRequest } from "next/server";
import { authorizeUrl, googleEnv, safeNext, STATE_COOKIE } from "@/lib/auth/google";

export async function GET(request: NextRequest) {
  const { origin, searchParams } = request.nextUrl;
  const env = googleEnv();
  if (!env) return NextResponse.redirect(`${origin}/login?error=config`);

  const state = crypto.randomUUID();
  const response = NextResponse.redirect(authorizeUrl(env.clientId, origin, state));
  // CSRF: the same state must come back to the callback.
  response.cookies.set(STATE_COOKIE, JSON.stringify({ state, next: safeNext(searchParams.get("next")) }), {
    httpOnly: true,
    secure: origin.startsWith("https://"),
    sameSite: "lax",
    path: "/",
    maxAge: 600,
  });
  return response;
}
