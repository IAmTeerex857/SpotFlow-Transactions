// The single source of truth for providers, regions, statuses and channels.
// The Python pipeline kept four diverging copies of these lists; every new
// market meant four edits. Uploads that reference a value outside this
// registry are surfaced in the upload summary instead of being dropped.

// The 11 providers the Python parser knew. Kept separately so the parity test
// can reproduce the historical reports exactly.
const LEGACY_PROVIDERS = [
  "cellulant",
  "hubtel",
  "interswitch",
  "ozow",
  "paystack",
  "spotflow_accounts",
  "tembo_plus",
  "precium",
  "hub2",
  "kashier",
  "pawapay",
] as const;

// Every provider in the Metabase service_providers mapping.
const PROVIDERS = [
  ...LEGACY_PROVIDERS,
  "flutterwave",
  "korapay",
  "nsano",
  "peachpay",
  "honeycoin",
  "hyperrail",
  "opay",
] as const;

// Display order matches the Python report.
const REGIONS = [
  "Nigeria",
  "Ghana",
  "South Africa",
  "Kenya",
  "Tanzania",
  "Côte d'Ivoire",
  "Benin",
  "Togo",
  "Egypt",
  "Cameroon",
  "Uganda",
  "Rwanda",
  "Congo",
  "Sierra Leone",
  "Democratic Republic of the Congo",
  "Zambia",
] as const;

// Raw spellings seen in exports, mapped to the canonical region name.
const REGION_ALIASES: Record<string, string> = {
  RWANDA: "Rwanda",
  "C��te d'Ivoire": "Côte d'Ivoire",
  "C�te d'Ivoire": "Côte d'Ivoire",
};

export const STATUSES = ["successful", "failed", "abandoned", "cancelled", "refunded", "inprogress"] as const;
export const CHANNELS = ["card", "bank_transfer", "mobile_money", "eft"] as const;
export const CURRENCIES = [
  "USD", "NGN", "GHS", "XAF", "KES", "TZS", "ZAR", "XOF", "EGP", "UGX", "ZMW", "RWF", "SLE", "CDF",
] as const;

export type Status = (typeof STATUSES)[number];
export type Channel = (typeof CHANNELS)[number] | "other";

export interface Registry {
  providers: ReadonlySet<string>;
  /** Exact raw spelling → canonical region name. */
  regions: ReadonlyMap<string, string>;
}

export function buildRegistry(options: {
  providers?: readonly string[];
  regions?: readonly string[];
  extraProviders?: readonly string[];
  extraRegions?: readonly string[];
} = {}): Registry {
  const providers = new Set<string>(
    [...(options.providers ?? PROVIDERS), ...(options.extraProviders ?? [])].map((p) => p.trim().toLowerCase()),
  );
  const regions = new Map<string, string>();
  for (const region of [...(options.regions ?? REGIONS), ...(options.extraRegions ?? [])]) {
    regions.set(region, region);
  }
  for (const [alias, canonical] of Object.entries(REGION_ALIASES)) {
    if (regions.has(canonical)) regions.set(alias, canonical);
  }
  return { providers, regions };
}

export const DEFAULT_REGISTRY = buildRegistry();
export const LEGACY_REGISTRY = buildRegistry({ providers: LEGACY_PROVIDERS });

export function regionOrder(region: string): number {
  const idx = (REGIONS as readonly string[]).indexOf(region);
  return idx === -1 ? REGIONS.length : idx;
}

