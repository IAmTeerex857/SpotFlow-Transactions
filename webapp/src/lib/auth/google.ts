import "server-only";
import { MEMBER_DOMAIN } from "@/lib/supabase/env";

// Google sign-in reuses the OAuth client and the redirect URI
// (<origin>/api/auth/callback) that the original reports site registered, so
// nothing changes in Google Cloud. The ID token is then handed to Supabase.

export const STATE_COOKIE = "sf_oauth";

export function googleEnv() {
  const clientId = process.env.GOOGLE_CLIENT_ID;
  const clientSecret = process.env.GOOGLE_CLIENT_SECRET;
  return clientId && clientSecret ? { clientId, clientSecret } : null;
}

export const callbackUrl = (origin: string) => `${origin}/api/auth/callback`;

export function authorizeUrl(clientId: string, origin: string, state: string) {
  const params = new URLSearchParams({
    client_id: clientId,
    redirect_uri: callbackUrl(origin),
    response_type: "code",
    scope: "openid email profile",
    hd: MEMBER_DOMAIN, // preselects the workspace; enforcement happens after
    prompt: "select_account",
    state,
  });
  return `https://accounts.google.com/o/oauth2/v2/auth?${params}`;
}

/** Exchanges the one-time code for Google's ID token (server to server, over TLS). */
export async function exchangeCode(code: string, origin: string): Promise<string> {
  const env = googleEnv();
  if (!env) throw new Error("Google sign-in is not configured");
  const res = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      code,
      client_id: env.clientId,
      client_secret: env.clientSecret,
      redirect_uri: callbackUrl(origin),
      grant_type: "authorization_code",
    }),
  });
  const body = (await res.json().catch(() => ({}))) as { id_token?: string; error?: string };
  if (!res.ok || !body.id_token) throw new Error(`Google token exchange failed: ${body.error ?? res.status}`);
  return body.id_token;
}

/** Reads the claims of a token that came straight from Google's token endpoint. */
export function idTokenClaims(token: string): { email?: string; email_verified?: boolean | string } {
  try {
    return JSON.parse(Buffer.from(token.split(".")[1], "base64url").toString("utf8"));
  } catch {
    return {};
  }
}

/** Only same-site app paths are allowed as the post-login destination. */
export function safeNext(value: string | null | undefined): string {
  return value && value.startsWith("/app") && !value.startsWith("//") ? value : "/app";
}
