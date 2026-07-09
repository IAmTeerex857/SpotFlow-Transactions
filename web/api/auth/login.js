// Start the Google OAuth flow: redirect the visitor to Google's sign-in.
import { randomBytes } from 'node:crypto';

export default function handler(req, res) {
  const clientId = process.env.GOOGLE_CLIENT_ID;
  if (!clientId) {
    res.status(500).send('Auth not configured (GOOGLE_CLIENT_ID missing).');
    return;
  }

  const redirectUri = `${originOf(req)}/api/auth/callback`;
  const state = randomBytes(16).toString('hex');

  // Hint Google to preselect the corporate domain (enforcement happens in the
  // callback — this is only a UX hint).
  const primaryDomain = (process.env.ALLOWED_EMAIL_DOMAINS || '')
    .split(',')[0]
    .trim();

  const params = new URLSearchParams({
    client_id: clientId,
    redirect_uri: redirectUri,
    response_type: 'code',
    scope: 'openid email profile',
    state,
    access_type: 'online',
    prompt: 'select_account',
  });
  if (primaryDomain) params.set('hd', primaryDomain);

  res.setHeader(
    'Set-Cookie',
    `sf_oauth_state=${state}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=600`,
  );
  res.writeHead(302, {
    Location: `https://accounts.google.com/o/oauth2/v2/auth?${params}`,
  });
  res.end();
}

function originOf(req) {
  const proto = (req.headers['x-forwarded-proto'] || 'https').split(',')[0].trim();
  const host = req.headers['x-forwarded-host'] || req.headers.host;
  return `${proto}://${host}`;
}
