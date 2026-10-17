# Spotflow Insights

Spotflow members sign in with Google (`@spotflow.one` only), upload a transactions export and get the analysed report: where payments fail, why, by provider, region and merchant, plus customer retry behaviour. It replaces the laptop pipeline in `../Spotflow transactions data/` and the static reports in `../web/`.

| Route | What it is |
|---|---|
| `/` | Public landing page (no data; CSS-3D isometric hero) |
| `/demo`, `/demo/reports` | Unlinked preview of Home and Reports on synthetic data, for working on the UI without Supabase |
| `/login` | Google sign-in |
| `/app` | Home: KPIs vs the previous day, quick actions, what changed, failure reasons, provider health, recent uploads |
| `/app/reports` | Explorer: any date range and filters (shareable by URL), merchants/providers/regions tables with detail drawers, failures, retries, peak times |
| `/app/upload` | Node-flow canvas: parse in the browser → review → save |
| `/app/uploads` | Upload history, per-upload report, reprocess, delete |
| `/app/ask` | Questions answered by an LLM on the Vercel AI Gateway, using read-only queries |

Press ⌘K anywhere in the app to search pages and report views or ask a question; ⌘B collapses the sidebar. Report tabs are linkable (`/app/reports#providers`).

## Design

Light by default with a full dark mode (toggle in the top bar). Inter for text, Geist for headings; one blue accent (`#3366FF`). Chart status colours are validated for colour-blind separation in both themes (`--chart-*` in `globals.css`; abandoned is golden amber).

The landing page has no data on it. The hero (`landing/hero/hero-scene.tsx`) is DOM on a single CSS-3D plane: tiles follow `offset-path` routes into the Spotflow disc and report cards slide out, with a slight cursor parallax. How it works and Security play short scripted vignettes (fictional names and numbers). Illustrations are isometric SVG (`landing/art/iso.tsx`). All motion pauses off screen and stops under reduced motion. Button and text interactions follow amicro (`ui/morph-button.tsx`, `landing/motion.tsx`).

## Project structure

```
src/
  app/                    routes (landing, login, /app, /demo, /api, /auth) and globals.css
  proxy.ts                session refresh and the /app + /api gate
  components/
    ui/                   primitives: MorphButton, Card, Popover, tabs, status pills, theme toggle
    shell/                app chrome: sidebar, top bar, ⌘K menu, page header
    home/ report/ upload/ ask/ charts/ auth/ brand/   one folder per product area
    landing/              nav, footer, motion helpers, landing.css
      hero/ sections/ art/
  hooks/                  shared client hooks
  lib/
    parser/               CSV reader and export parser (Python parity)
    report/               cube → report, filters, home summary, HTML export
    ingest/               pseudonymisation, masking, rows for the database
    server/               server-only: auth, report data, uploads, registry, ingest
    supabase/             clients and env
    ask/                  prompt and tools for Ask
    demo/                 synthetic data for /demo
supabase/                 migration and database tests (PGlite)
scripts/                  history import, golden-file dump, screenshots
```

## How it fits together

```
Browser ── CSV ──► parse (TypeScript, in the browser) ── review
              └─ gzip ─► POST /api/uploads ─► parse again on the server
                                              ├─ emails → HMAC pseudonyms, phone numbers masked
                                              ├─ ingest_transactions() → Postgres (RLS)
                                              └─ scrubbed copy → Storage (for reprocessing)
Reports ◄── report_cube() (one SQL call per view) ◄── transactions
```

- **Parser** (`src/lib/parser`): a port of `analyze_transactions.py` + `render_html_report_v2.py`, including recovery of transactions merged onto one CSV line. `src/lib/parser/parity.test.ts` checks it against the Python output for every historical export (315 files, 263,907 transactions; identical).
- **Registry** (`src/lib/parser/registry.ts`): one list of providers and regions. Unknown values in an upload are shown in the review step and can be added from there, instead of being silently dropped.
- **Database** (`supabase/migrations`): tables, row-level security, the domain trigger, `ingest_transactions`, `report_cube`. `supabase/db.test.ts` runs the real migration in PGlite and checks the SQL report equals the TypeScript report.
- **Re-uploads are idempotent**: each transaction has a stable key (the export's Reference when present), so uploading the same export twice updates rather than duplicates.

### Fixes over the Python pipeline
- Retry "still unresolved" now uses each customer's *latest* attempt by time (the Python report read the oldest, because exports are newest-first).
- Customer pseudonyms are stable across days (HMAC), so weekly and monthly retry analysis no longer merges different people who shared a per-day "customer N" label. Historical imports keep day-scoped ids and the UI says so.
- Seven providers from the Metabase mapping (flutterwave, korapay, nsano, peachpay, honeycoin, hyperrail, opay) are now recognised.
- Phone numbers and emails echoed inside provider messages are masked before storage.
- Report HTML is escaped (merchant names and messages come from uploaded files).

## Setup

### 1. Supabase
1. Apply `supabase/migrations/20261017000000_init.sql` (SQL Editor, `supabase db push`, or the Management API's `database/query`). It creates the tables, row-level security, the domain trigger, the report functions and the private `exports` bucket.
2. **Authentication → Providers → Google**: enable it with the same Google OAuth client ID and secret as `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET`. Supabase only verifies the ID token's audience; no Supabase redirect URI is registered in Google.

Non-`@spotflow.one` Google accounts are refused three times over: Google's `hd` hint, the check in `/api/auth/callback`, and a database trigger that blocks the user row. Every table policy re-checks the domain.

### 2. Google sign-in
Sign-in reuses the reports site's existing OAuth client and its registered redirect URI, `https://<domain>/api/auth/callback`. `/api/auth/login` sends the member to Google; the callback exchanges the code for an ID token, checks the domain and calls `supabase.auth.signInWithIdToken`. Nothing changes in Google Cloud. Preview deployments have other domains, so sign in on production (or add their callback URL to the client).

### 3. Environment and Vercel
`.env.example` lists every variable. In Vercel (Root Directory `webapp`, framework Next.js) set `NEXT_PUBLIC_SUPABASE_URL`, `NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY`, `PSEUDONYM_SECRET`, `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET` and optionally `ASK_MODEL`. Never put the service-role key in Vercel. Ask needs no key on Vercel: the AI Gateway authenticates with the deployment's OIDC token.

### 4. Import history (once)
```bash
npm run import:history -- --dry-run      # preview
npm run import:history                   # imports ../data/**/*anonymized.csv
```
Safe to re-run: files already imported are skipped.

## Development
```bash
npm install
npm run dev          # http://localhost:3000 (works without Supabase: try /demo)
npm test             # parser parity + database tests
npm run golden       # regenerate .golden/python.json from the Python parser
npm run typecheck && npm run lint
npm run screenshots -- http://localhost:3000   # light/dark, desktop/phone, flags console errors and overflow
```
The parity test needs the local `../data` folder and `.golden/python.json`; it is skipped elsewhere.

## Ask
`/api/ask` streams answers through the Vercel AI Gateway (`deepseek/deepseek-v4.1-flash` by default; any tool-calling model via `ASK_MODEL`) with the AI SDK's `streamText`. Six read-only tools call the same SQL as the dashboard using the signed-in member's session, so row-level security applies, and the loop stops after ten steps.
