# Google Auth Gate — Setup Runbook

Only Google accounts on the allowed domain(s) can view reports. The landing page
(`/`) is public; `/reports.html` and everything under `/reports/**` are gated by
`middleware.js`.

## How it works
```
Visitor → /reports…                         (any report URL)
   │  Edge middleware: valid sf_session cookie?
   ├─ yes → serve the report ✅
   └─ no  → / (landing) → "Sign in with Google"
                 → /api/auth/login   → Google
                 → /api/auth/callback  (checks domain, sets cookie)
                       ├─ @spotflow → cookie set → /reports.html ✅
                       └─ other     → /?denied=domain ✋
```

## One-time setup

### 1. Create the Google OAuth client
1. https://console.cloud.google.com → create/select a project.
2. **APIs & Services → OAuth consent screen** → Internal (if this is a Google
   Workspace org) → fill app name + support email.
3. **APIs & Services → Credentials → Create Credentials → OAuth client ID**
   - Application type: **Web application**
   - **Authorized redirect URI:**
     `https://spot-flow-transactions.vercel.app/api/auth/callback`
     (add any custom domain's `/api/auth/callback` too)
4. Copy the **Client ID** and **Client secret**.

### 2. Set Vercel environment variables
Vercel → your project → **Settings → Environment Variables** (Production +
Preview). Nothing secret goes in the code — only here.

| Name | Value |
|------|-------|
| `GOOGLE_CLIENT_ID` | *(from step 1)* |
| `GOOGLE_CLIENT_SECRET` | *(from step 1)* |
| `SESSION_SECRET` | a long random string — e.g. `7Lb2i0F37lmoz3SdV6dveytSm20roRloZ0HkwYWm4uWEvyIIsAVYkSqcYrsizluz` (regenerate your own for production) |
| `ALLOWED_EMAIL_DOMAINS` | comma-separated domains, e.g. `spotflow.xyz` (confirm the real domain) |

### 3. Vercel project settings
- **Root Directory:** `web` (already set)
- **Framework Preset:** Other
- **Build Command:** *(leave empty)*
- **Output Directory:** *(leave empty / default)*
- Vercel auto-runs `npm install` (pulls `jose` + `@vercel/edge`).

### 4. Deploy
Push to `main`. Vercel builds, wires up the Edge Middleware + `/api/auth/*`
functions, and the gate goes live.

### 5. Close the back door
Disable the GitHub Pages mirror — otherwise there's an ungated public copy of the
reports that bypasses this gate:
```
gh api -X DELETE repos/IAmTeerex857/SpotFlow-Transactions/pages
```

## ⚠️ Rollout order matters
The gate is **fail-closed**. If the auth code is deployed *before* the env vars
exist, reports become inaccessible to everyone. So: **set the env vars in Vercel
first, then push the auth code.**

## Local files
- `middleware.js` — the gate (Edge)
- `api/auth/login.js` — start Google sign-in
- `api/auth/callback.js` — verify domain, issue session cookie
- `api/auth/logout.js` — clear session
- `package.json` — declares `jose` + `@vercel/edge`
