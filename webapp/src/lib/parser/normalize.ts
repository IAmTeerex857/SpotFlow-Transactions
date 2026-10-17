// Port of `_normalise_message` from analyze_transactions.py. Provider wording
// varies for the same underlying failure; these buckets combine equivalent
// errors without hiding materially different causes.

const BLANK_MESSAGE = "<blank>";
const REDACTED_SECRET = "[redacted]";

const SECRET_PATTERNS: [RegExp, boolean][] = [
  [/\b(bearer)\s+[A-Za-z0-9._~+/=-]{20,}/gi, true],
  [/\b(token|authorization|access[_ -]?token|api[_ -]?key)\s*[:=]?\s+[A-Za-z0-9._~+/=-]{20,}/gi, true],
  [/\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b/g, false],
];

const OTP_PREFIXES = ["kindly enter the otp", "please enter the otp", "please input the otp"];

const GROUPED_PATTERNS: [string, RegExp[]][] = (
  [
    ["Insufficient funds", [
      String.raw`insufficient funds`,
      String.raw`payment failed\. the customer doesn't have enough funds`,
      String.raw`the customer does not have enough funds to complete the payment`,
    ]],
    ["Insufficient funds or payment limit reached", [
      String.raw`the customer does not have enough funds to complete the payment or has reached a payment limit`,
      String.raw`low balance or payee limit reached or not allowed`,
      String.raw`the MTN mobile money user has reached counter or balance limits`,
    ]],
    ["Payment authorization timed out", [
      String.raw`payment failed\. the waiting period for payment execution has expired`,
      String.raw`the customer did not authorize the payment in time or the transaction timed out at the mobile network operator`,
      String.raw`your payment has exceeded the time required to pay`,
      String.raw`authorization not approved in time`,
      String.raw`timed out`,
    ]],
    ["Provider returned no failure reason", [
      String.raw`payment failed\. no reason was provided by the provider`,
      String.raw`the provider '.+' did not specify a reason for this failure`,
    ]],
    ["Transaction failed", [
      String.raw`transaction failed`,
      String.raw`payment failed`,
      String.raw`provider failed`,
      String.raw`generic failure`,
      String.raw`the vodafone cash failed`,
      String.raw`failed`,
      String.raw`failure`,
    ]],
    ["Unable to process transaction", [
      String.raw`error processing payment`,
      String.raw`unable to process transaction(?: at this time)?`,
      String.raw`unable to perform transaction`,
      String.raw`transaction error`,
      String.raw`card could not be processed at this time`,
      String.raw`provider can't process merchant's payment`,
    ]],
    ["Provider temporarily unavailable", [
      String.raw`payment failed\. provider is currently unavailable`,
      String.raw`payment failed\. the service is temporarily disabled due to an ongoing incident\. our technical service is aware and is working on its resolution`,
      String.raw`the provider '.+' is currently not able to process payments\. please consult our status page for downtime information on all providers\. programmatic access is also available`,
      String.raw`exception happened with upstream provider`,
    ]],
    ["OTP verification required", [
      String.raw`OTP verification required`,
      String.raw`OTP is required when .+ provider is selected`,
    ]],
    ["Incorrect PIN", [
      String.raw`incorrect PIN`,
      String.raw`you entered the wrong PIN\. please try again`,
    ]],
    ["Payer account not found or inactive", [
      String.raw`the payer does not have an account with '.+' or it is not active`,
      String.raw`your account is not a valid vodafone cash account`,
      String.raw`inactive or dormant account\. please contact issuer`,
    ]],
    ["Payment rejected by provider", [
      String.raw`payment rejected`,
      String.raw`transaction declined`,
      String.raw`payment failed\. the payment was forbidden by the provider`,
    ]],
    ["Invalid phone number", [
      String.raw`payment failed\. incorrect phone number`,
      String.raw`the msisdn is too (?:short|long) for the identified country .+`,
      String.raw`country code resolved from msisdn is not valid`,
    ]],
    ["Amount precision not supported", [
      String.raw`the provider '.+' only supports up to '\d+' decimal places in amount`,
    ]],
  ] as [string, string[]][]
).map(([label, patterns]) => [label, patterns.map((p) => new RegExp(`^(?:${p})$`, "i"))]);

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

// Python's str.title(): uppercase a letter that follows a non-letter.
function pyTitle(value: string): string {
  let out = "";
  let prevCased = false;
  for (const ch of value) {
    const isLetter = ch.toLowerCase() !== ch.toUpperCase();
    out += isLetter ? (prevCased ? ch.toLowerCase() : ch.toUpperCase()) : ch;
    prevCased = isLetter;
  }
  return out;
}

// Python's str.strip(chars) for a given set of characters.
function stripChars(value: string, chars: string): string {
  let start = 0;
  let end = value.length;
  while (start < end && chars.includes(value[start])) start += 1;
  while (end > start && chars.includes(value[end - 1])) end -= 1;
  return value.slice(start, end);
}

function redactSecrets(value: string): string {
  let out = value;
  for (const [pattern, hasGroup] of SECRET_PATTERNS) {
    pattern.lastIndex = 0;
    out = hasGroup
      ? out.replace(pattern, (_m, g1: string) => `${g1} ${REDACTED_SECRET}`)
      : out.replace(pattern, REDACTED_SECRET);
  }
  return out;
}

export function normaliseMessage(message: string | null | undefined): string {
  if (message == null) return BLANK_MESSAGE;

  const cleaned = message.replace(/\r\n/g, "\n").trim();
  if (!cleaned || [",", ",,", '""', "''"].includes(cleaned)) return BLANK_MESSAGE;

  let firstLine = cleaned.split("\n", 1)[0].trim();
  if (firstLine.startsWith('"') && firstLine.endsWith('"') && firstLine.length > 1) {
    firstLine = firstLine.slice(1, -1).trim();
  }
  firstLine = stripChars(firstLine, '"').trim();

  if (!firstLine || [",", ",,", ".", "-"].includes(firstLine)) return BLANK_MESSAGE;

  firstLine = firstLine.replace(/ /g, " ");
  firstLine = firstLine.replace(/\s+/g, " ").trim();
  firstLine = redactSecrets(firstLine);

  const stray = firstLine.match(/^(.*?)"\s*,/s);
  if (stray && stray[1].trim()) firstLine = stray[1].trim();

  firstLine = firstLine.replace(/[\s,;:."-]+$/, "").trim();

  if (firstLine.includes("_")) {
    const candidate = firstLine.replace(/_/g, " ").trim();
    if (candidate) {
      firstLine = firstLine.toUpperCase() === firstLine ? pyTitle(candidate) : candidate;
    }
  }

  let lower = firstLine.toLowerCase();
  if (OTP_PREFIXES.some((p) => lower.startsWith(p))) return "OTP verification required";

  const uppercaseTokens = new Set<string>();
  for (const match of firstLine.matchAll(/\b[A-Z]{2,}\b/g)) {
    if (match[0].length <= 4) uppercaseTokens.add(match[0]);
  }
  for (const token of uppercaseTokens) {
    lower = lower.replace(new RegExp(`\\b${escapeRegExp(token.toLowerCase())}\\b`, "g"), token);
  }

  firstLine = lower ? lower[0].toUpperCase() + lower.slice(1) : lower;

  for (const [label, patterns] of GROUPED_PATTERNS) {
    if (patterns.some((p) => p.test(firstLine))) return label;
  }
  return firstLine;
}

export function displayMessage(message: string): string {
  return message === BLANK_MESSAGE ? "No provider message (blank)" : message;
}
