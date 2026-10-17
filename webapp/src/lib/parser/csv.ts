// A CSV reader that matches Python's `csv.reader` with the default dialect
// (doublequote=True, strict=False). Fragmented Spotflow rows depend on its
// exact quirks: a quote that does not open a field is literal, and text after
// a closing quote is appended to the field instead of raising.

export function* readCsv(text: string): Generator<string[]> {
  const n = text.length;
  let i = 0;

  while (i < n) {
    const row: string[] = [];
    let field = "";
    let inQuotes = false;
    let fieldStart = true;
    let lineEnded = false;

    while (i < n) {
      const ch = text[i];

      if (inQuotes) {
        if (ch === '"') {
          if (text[i + 1] === '"') {
            field += '"';
            i += 2;
            continue;
          }
          inQuotes = false;
          i += 1;
          continue;
        }
        field += ch;
        i += 1;
        continue;
      }

      if (ch === '"' && fieldStart) {
        inQuotes = true;
        fieldStart = false;
        i += 1;
        continue;
      }
      if (ch === ",") {
        row.push(field);
        field = "";
        fieldStart = true;
        i += 1;
        continue;
      }
      if (ch === "\r" || ch === "\n") {
        i += ch === "\r" && text[i + 1] === "\n" ? 2 : 1;
        lineEnded = true;
        break;
      }
      field += ch;
      fieldStart = false;
      i += 1;
    }

    // Python yields [] for a blank line and a single field otherwise.
    if (row.length === 0 && field === "" && !inQuotes && fieldStart) {
      if (lineEnded || i < n) yield [];
      continue;
    }
    row.push(field);
    yield row;
  }
}

export function stripBom(text: string): string {
  return text.charCodeAt(0) === 0xfeff ? text.slice(1) : text;
}

const NEEDS_QUOTING = /[",\r\n]/;

export function writeCsv(rows: readonly (readonly string[])[]): string {
  return rows
    .map((row) => row.map((cell) => (NEEDS_QUOTING.test(cell) ? `"${cell.replace(/"/g, '""')}"` : cell)).join(","))
    .join("\r\n") + "\r\n";
}
