// Vercel Edge Middleware — the gate.
//
// Runs *before* any report is served. Only requests carrying a valid session
// cookie (issued by /api/auth/callback after a Spotflow Google sign-in) are
// allowed through to the report content; everyone else is bounced to the
// landing page to sign in.
//
// Fail-closed: if SESSION_SECRET isn't configured, or the cookie is missing/
// invalid/expired, access is denied.

import { next } from '@vercel/edge';
import { jwtVerify } from 'jose';

// Only guard the report content. The landing page, styles, assets and the
// /api/auth/* endpoints stay public.
export const config = {
  matcher: ['/reports.html', '/reports/:path*'],
};

export default async function middleware(request) {
  const secret = process.env.SESSION_SECRET;
  const token = readCookie(request.headers.get('cookie'), 'sf_session');

  if (secret && token) {
    try {
      await jwtVerify(token, new TextEncoder().encode(secret));
      return next(); // valid session — serve the report
    } catch {
      // fall through to redirect
    }
  }

  const home = new URL('/', request.url);
  home.searchParams.set('denied', 'auth');
  return Response.redirect(home, 307);
}

function readCookie(header, name) {
  if (!header) return null;
  for (const part of header.split(';')) {
    const eq = part.indexOf('=');
    if (eq === -1) continue;
    if (part.slice(0, eq).trim() === name) {
      return decodeURIComponent(part.slice(eq + 1).trim());
    }
  }
  return null;
}
