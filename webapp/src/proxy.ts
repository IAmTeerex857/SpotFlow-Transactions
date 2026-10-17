// Refreshes the Supabase session on every request and keeps signed-out
// visitors out of the app. This is an optimistic gate: pages and API routes
// verify the session again, and row level security enforces it in Postgres.
import { createServerClient } from "@supabase/ssr";
import { NextResponse, type NextRequest } from "next/server";
import { isMemberEmail, supabaseEnv } from "@/lib/supabase/env";

export async function proxy(request: NextRequest) {
  let response = NextResponse.next({ request });
  const env = supabaseEnv();
  const { pathname } = request.nextUrl;
  // The Google sign-in routes are how members get a session, so they stay open.
  const isProtected = pathname.startsWith("/app") || (pathname.startsWith("/api/") && !pathname.startsWith("/api/auth/"));

  if (!env) {
    if (!isProtected) return response;
    return pathname.startsWith("/api/")
      ? NextResponse.json({ error: "Supabase is not configured" }, { status: 503 })
      : NextResponse.redirect(new URL("/login?error=config", request.url));
  }

  const supabase = createServerClient(env.url, env.key, {
    cookies: {
      getAll() {
        return request.cookies.getAll();
      },
      setAll(cookiesToSet, headers) {
        for (const { name, value } of cookiesToSet) request.cookies.set(name, value);
        response = NextResponse.next({ request });
        for (const { name, value, options } of cookiesToSet) response.cookies.set(name, value, options);
        for (const [key, value] of Object.entries(headers ?? {})) response.headers.set(key, value);
      },
    },
  });

  const { data } = await supabase.auth.getClaims();
  const member = isMemberEmail(data?.claims?.email as string | undefined);

  if (isProtected && !member) {
    if (pathname.startsWith("/api/")) return NextResponse.json({ error: "Sign in with a spotflow.one account" }, { status: 401 });
    const url = new URL("/login", request.url);
    url.searchParams.set("next", pathname + request.nextUrl.search);
    return NextResponse.redirect(url);
  }
  if (pathname === "/login" && member) return NextResponse.redirect(new URL("/app", request.url));

  return response;
}

export const config = {
  matcher: ["/app/:path*", "/api/:path*", "/login"],
};
