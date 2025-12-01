# Spotflow Transaction Analysis Guide

This folder centralises the tooling and documentation needed to analyse the daily
Spotflow exports that arrive in the `Failed Payments` workspace.

## 1. Workflow Overview

1. Drop the raw export (`*.csv`) into `Failed Payments/`.
2. Run `python analyze_transactions.py /absolute/path/to/export.csv`.
3. Review the console output for:
   - Aggregate totals across all regions
   - Regional breakdowns for Nigeria, Ghana, South Africa, Kenya and Tanzania
   - Provider-by-provider success and failure narratives per region

The script can be re-used for any of the daily exports and is resilient to the
malformed lines we previously identified (rows that contain multiple
transactions stitched together without proper quoting).

## 2. Parsing the CSV Correctly

Spotflow exports are not always clean single-row records. Issues to watch out for:

- **Merged transactions on one line:** Some rows contain several complete
  transactions separated by commas. They are usually introduced when Hubtel or
  Spotflow retries append extra context (e.g. `Audiomack Inc.` blocks). The
  script scans each row for sequences that look like `provider → region → status →
  channel`, and treats each match as a distinct transaction.
- **Optional `Rate` column:** Certain files add a third column between
  `Currency` and `Provider`. The parser does not rely on fixed column positions;
  it looks for known providers/regions and walks forward from there.
- **Missing payment dates:** When a merged row repeats the `Provider Message` but
  omits a second timestamp, the script assigns a placeholder ISO string so the
  record is still counted. Any entry without a genuine ISO timestamp is clearly
  flagged in the output (`<missing-date>`).
- **Unexpected channels:** If a channel does not match `card`, `bank_transfer`,
  `eft`, or `mobile_money`, it is bucketed under `other` so totals remain
  consistent.

## 3. Expected Results Per Export

Every run should surface the following metrics:

- **Aggregate (all regions)**
  - Date/time range of the transactions contained in the export
  - Counts for `successful`, `failed`, `abandoned`, `cancelled`, `inprogress`
  - Channel totals (card, bank transfer, EFT, mobile money, other)
  - Top success messages (e.g., `success`, `successful`) and top failure messages
    (e.g., OTP prompts, provider errors)
  - Provider breakdown covering overall volume per provider plus their leading
    success/failure reasons

- **Per Region (Nigeria, Ghana, South Africa, Kenya, Tanzania)**
  - Same structure as the aggregate summary: status counts, channel mix, top
    messages, provider breakdowns
  - Blank provider messages are reported separately so we know where we lack
    detail

- **Per Provider (within each region and aggregate)**
  - Status counts (how many successes, failures, etc.)
  - Channel usage within that provider
  - Top success messages (typically echoes like `successful`) and top failure
    messages (OTP prompts, timeouts, insufficient funds, etc.)

## 4. Provider-Level Diagnostics

The provider breakdown printed inside each region highlights actionable items:

- **Success drivers:** Useful to confirm which providers are actually converting
  (e.g., Hubtel success vs. Spotflow success echoes).
- **Failure drivers:** Surfaces the most common reasons for failure so we can
  prioritise fixes (OTP loop, `Request to Generate Token is Successful`, EFT
  timeouts, Tembo Plus `PROVIDER_FAILED`, etc.).

Because the script normalises blank provider messages to `"<blank>"`, we can
quickly spot integrations that need debugging at source.

## 5. Running the Script

```bash
cd "/Users/casper/Failed Payments/Spotflow transactions data"
python analyze_transactions.py "/Users/casper/Failed Payments/Nov 14 - export (36).csv.csv"
```

Optional flags:

- `--top N` – change how many success/failure messages are displayed per section
  (default is 5).

The script exits with a non-zero status if it cannot parse the file so we can
plug it into automated checks later.

## 6. Extending the Analysis

- **Exporting to CSV/JSON:** Wrap `parse_csv` to dump structured outputs if you
  need to feed dashboards.
- **Date filtering:** The current logic keeps any ISO timestamp inside the file.
  Add explicit date filters if future exports span multiple days.
- **Additional providers/regions:** Update `PROVIDERS` or `REGIONS` sets at the
  top of `analyze_transactions.py` when new markets go live.

Keep this README updated whenever the analysis requirements evolve so future
audits remain consistent.
