// Google OAuth callback: verify the sign-in, enforce the Spotflow domain,
// and issue a signed session cookie.
import { SignJWT } from 'jose';

const SESSION_DAYS = 7;

export default async function handler(req, res) {
  const { GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, SESSION_SECRET } = process.env;
  const allowedDomains = (process.env.ALLOWED_EMAIL_DOMAINS || '')
    .split(',')
    .map((d) => d.trim().toLowerCase())
    .filter(Boolean);

  if (!GOOGLE_CLIENT_ID || !GOOGLE_CLIENT_SECRET || !SESSION_SECRET) {
    res.status(500).send('Auth not configured (missing env vars).');
    return;
  }

  const url = new URL(req.url, 'https://placeholder.local');
  const code = url.searchParams.get('code');
  const state = url.searchParams.get('state');
  const cookies = parseCookies(req.headers.cookie);

  // CSRF: the state we set at /login must come back unchanged.
  if (!code || !state || state !== cookies.sf_oauth_state) {
    return deny(res, 'state');
  }

  const redirectUri = `${originOf(req)}/api/auth/callback`;

  // Exchange the one-time code for tokens (uses the client secret, server-side).
  let idToken;
  try {
    const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        code,
        client_id: GOOGLE_CLIENT_ID,
        client_secret: GOOGLE_CLIENT_SECRET,
        redirect_uri: redirectUri,
        grant_type: 'authorization_code',
      }),
    });
    if (!tokenRes.ok) return deny(res, 'token');
    idToken = (await tokenRes.json()).id_token;
  } catch {
    return deny(res, 'token');
  }
  if (!idToken) return deny(res, 'token');

  // The id_token comes straight from Google over TLS in a server-to-server
  // exchange, so we can trust its payload without re-fetching JWKS.
  const claims = decodeJwtPayload(idToken);
  const email = String(claims.email || '').toLowerCase();
  const verified = claims.email_verified === true || claims.email_verified === 'true';
  const domainOk =
    allowedDomains.length > 0 &&
    allowedDomains.some((d) => email.endsWith(`@${d}`));

  if (!email || !verified || !domainOk) {
    return deny(res, 'domain');
  }

  // Issue the session cookie.
  const jwt = await new SignJWT({ email })
    .setProtectedHeader({ alg: 'HS256' })
    .setIssuedAt()
    .setExpirationTime(`${SESSION_DAYS}d`)
    .sign(new TextEncoder().encode(SESSION_SECRET));

  res.setHeader('Set-Cookie', [
    `sf_session=${jwt}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=${SESSION_DAYS * 24 * 3600}`,
    'sf_oauth_state=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0',
  ]);
  res.writeHead(302, { Location: '/reports.html' });
  res.end();
}

function deny(res, reason) {
  res.setHeader(
    'Set-Cookie',
    'sf_oauth_state=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0',
  );
  res.writeHead(302, { Location: `/?denied=${reason}` });
  res.end();
}

function decodeJwtPayload(token) {
  try {
    return JSON.parse(Buffer.from(token.split('.')[1], 'base64url').toString('utf8'));
  } catch {
    return {};
  }
}

function parseCookies(header = '') {
  const out = {};
  for (const part of (header || '').split(';')) {
    const eq = part.indexOf('=');
    if (eq === -1) continue;
    out[part.slice(0, eq).trim()] = decodeURIComponent(part.slice(eq + 1).trim());
  }
  return out;
}

function originOf(req) {
  const proto = (req.headers['x-forwarded-proto'] || 'https').split(',')[0].trim();
  const host = req.headers['x-forwarded-host'] || req.headers.host;
  return `${proto}://${host}`;
}
