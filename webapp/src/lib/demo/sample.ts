// Synthetic, deterministic sample data for the /demo preview. Merchant names and
// numbers are invented; the shape (providers, regions, failure reasons, retry
// behaviour) mirrors real exports so the demo reads like the real product.
import type { CubeInput } from "@/lib/report/cube";

function rng(seed: number) {
  let s = seed >>> 0;
  return () => {
    s = (s + 0x6d2b79f5) >>> 0;
    let t = s;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

type Weighted<T> = [T, number][];
function pick<T>(r: () => number, items: Weighted<T>): T {
  const total = items.reduce((s, [, w]) => s + w, 0);
  let x = r() * total;
  for (const [item, w] of items) {
    if ((x -= w) <= 0) return item;
  }
  return items[items.length - 1][0];
}

interface Route {
  provider: string;
  region: string;
  channel: string;
  currency: string;
  rate: number;
  ok: number;
  fail: number;
  abandon: number;
  failures: Weighted<string>;
}

const ROUTES: Weighted<Route> = [
  [{ provider: "spotflow_accounts", region: "Nigeria", channel: "bank_transfer", currency: "NGN", rate: 1435.5, ok: 0.18, fail: 0.27, abandon: 0.55,
    failures: [["Payment verification failed after 3 retries", 7], ["Amount mismatch", 2], ["<blank>", 1]] }, 50],
  [{ provider: "hubtel", region: "Ghana", channel: "mobile_money", currency: "GHS", rate: 12.6, ok: 0.42, fail: 0.2, abandon: 0.38,
    failures: [["Insufficient funds", 5], ["Token [redacted] is locked - no attempts remaining", 2], ["Payment authorization timed out", 3], ["Incorrect PIN", 1]] }, 25],
  [{ provider: "paystack", region: "Nigeria", channel: "card", currency: "NGN", rate: 1435.5, ok: 0.31, fail: 0.52, abandon: 0.17,
    failures: [["Request to generate token is successful", 6], ["Insufficient funds", 2], ["Do not honor", 1], ["OTP verification required", 2]] }, 6],
  [{ provider: "pawapay", region: "Democratic Republic of the Congo", channel: "mobile_money", currency: "CDF", rate: 2394.9, ok: 0.36, fail: 0.58, abandon: 0.06,
    failures: [["The customer did not approve the payment in time.", 6], ["Insufficient funds", 3], ["Payment authorization timed out", 1]] }, 4],
  [{ provider: "hub2", region: "Côte d'Ivoire", channel: "mobile_money", currency: "XOF", rate: 566.2, ok: 0.44, fail: 0.4, abandon: 0.16,
    failures: [["Insufficient funds or payment limit reached", 4], ["Provider returned no failure reason", 2], ["Transaction failed", 2]] }, 5],
  [{ provider: "ozow", region: "South Africa", channel: "eft", currency: "ZAR", rate: 17.4, ok: 0.52, fail: 0.18, abandon: 0.3,
    failures: [["<blank>", 3], ["Transaction was cancelled", 1]] }, 2],
  [{ provider: "tembo_plus", region: "Tanzania", channel: "mobile_money", currency: "TZS", rate: 2650, ok: 0.47, fail: 0.4, abandon: 0.13,
    failures: [["Provider failed", 3], ["Insufficient funds", 2]] }, 2],
  [{ provider: "cellulant", region: "Uganda", channel: "mobile_money", currency: "UGX", rate: 3690, ok: 0.29, fail: 0.66, abandon: 0.05,
    failures: [["No payment methods configured for this service", 5], ["Payment authorization timed out", 2]] }, 2],
  [{ provider: "interswitch", region: "Kenya", channel: "card", currency: "KES", rate: 129.3, ok: 0.55, fail: 0.38, abandon: 0.07,
    failures: [["Unable to process transaction", 3], ["Payment rejected by provider", 2]] }, 1],
];

const MERCHANTS: Weighted<{ name: string; ticket: [number, number] }> = [
  [{ name: "Northwind Audio", ticket: [3, 8] }, 46],
  [{ name: "Kora Streaming", ticket: [4, 30] }, 20],
  [{ name: "Lumen Games", ticket: [1, 12] }, 15],
  [{ name: "Pavilion Tickets", ticket: [8, 60] }, 8],
  [{ name: "Tidewater Learning", ticket: [10, 45] }, 6],
  [{ name: "Ember Fitness", ticket: [5, 20] }, 5],
];

export function sampleTransactions(days = 28, end = "2026-10-15", seed = 7): CubeInput[] {
  const r = rng(seed);
  const rows: CubeInput[] = [];
  const endTime = Date.parse(`${end}T00:00:00Z`);
  let customerSeq = 0;
  for (let d = days - 1; d >= 0; d -= 1) {
    const dayStart = endTime - d * 86_400_000;
    const date = new Date(dayStart);
    const weekday = date.getUTCDay();
    const volume = Math.round((900 + r() * 500) * (weekday === 1 ? 1.25 : weekday === 6 ? 0.85 : 1));
    let i = 0;
    while (i < volume) {
      const route = pick(r, ROUTES);
      const merchant = pick(r, MERCHANTS);
      const customer = `demo${(customerSeq += 1)}`;
      // Afternoon-heavy traffic, like the real hour-of-day profile.
      const hour = Math.min(23, Math.max(0, Math.round(15 + (r() + r() + r() - 1.5) * 9)));
      let t = dayStart + hour * 3_600_000 + Math.floor(r() * 3_600_000);
      const attempts = r() < 0.18 ? 2 + Math.floor(r() * 3) : 1;
      for (let a = 0; a < attempts && i < volume; a += 1, i += 1) {
        const last = a === attempts - 1;
        let status = pick(r, [["successful", route.ok], ["failed", route.fail], ["abandoned", route.abandon], ["cancelled", 0.03]]);
        if (!last && status === "successful") status = "failed";
        if (last && attempts > 1 && r() < 0.35) status = "successful";
        const amountUsd = merchant.ticket[0] + r() * (merchant.ticket[1] - merchant.ticket[0]);
        const message = status === "successful" ? "Successful"
          : status === "failed" ? pick(r, route.failures)
            : status === "abandoned" ? "Payment abandoned after being pending for more than 1 hours"
              : "Transaction was cancelled";
        rows.push({
          merchant: merchant.name,
          provider: route.provider,
          region: route.region,
          status,
          channel: route.channel,
          currency: route.currency,
          amountUsd: Math.round(amountUsd * 100) / 100,
          message,
          paidAt: new Date(Math.min(t, dayStart + 86_399_000)).toISOString(),
          customerId: customer,
          businessDate: date.toISOString().slice(0, 10),
          seq: rows.length,
        });
        t += (2 + r() * 40) * 60_000;
      }
    }
  }
  return rows;
}

