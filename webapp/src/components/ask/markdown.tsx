// A deliberately small markdown renderer for assistant answers: paragraphs,
// headings, lists, tables, bold and inline code. It builds React elements,
// never HTML strings, so model output can't inject markup.
import { Fragment, type ReactNode } from "react";

function inline(text: string, key: string): ReactNode[] {
  const out: ReactNode[] = [];
  const re = /(\*\*[^*]+\*\*|`[^`]+`)/g;
  let last = 0;
  let i = 0;
  for (const m of text.matchAll(re)) {
    if (m.index! > last) out.push(text.slice(last, m.index));
    const token = m[0];
    out.push(token.startsWith("**")
      ? <strong key={`${key}-${i++}`} className="font-semibold text-ink">{token.slice(2, -2)}</strong>
      : <code key={`${key}-${i++}`} className="rounded bg-surface-2 px-1 py-0.5 font-mono text-[12px]">{token.slice(1, -1)}</code>);
    last = m.index! + token.length;
  }
  if (last < text.length) out.push(text.slice(last));
  return out;
}

const isTableRow = (line: string) => /^\s*\|.*\|\s*$/.test(line);
const cells = (line: string) => line.trim().replace(/^\||\|$/g, "").split("|").map((c) => c.trim());

export function Markdown({ text }: { text: string }) {
  const lines = text.split("\n");
  const blocks: ReactNode[] = [];
  let i = 0;
  while (i < lines.length) {
    const line = lines[i];
    if (!line.trim()) { i++; continue; }

    if (isTableRow(line) && i + 1 < lines.length && /^\s*\|?[\s:-]+\|/.test(lines[i + 1])) {
      const head = cells(line);
      const rows: string[][] = [];
      i += 2;
      while (i < lines.length && isTableRow(lines[i])) rows.push(cells(lines[i++]));
      blocks.push(
        <div key={`t${i}`} className="my-3 overflow-x-auto rounded-xl border border-line-soft scroll-quiet">
          <table className="w-full text-[12.5px]">
            <thead><tr className="bg-surface">{head.map((h, j) => <th key={j} className="px-3 py-2 text-left font-medium text-muted">{inline(h, `h${j}`)}</th>)}</tr></thead>
            <tbody className="tnum">{rows.map((r, ri) => <tr key={ri} className="border-t border-line-soft">{r.map((c, j) => <td key={j} className="px-3 py-1.5">{inline(c, `c${ri}${j}`)}</td>)}</tr>)}</tbody>
          </table>
        </div>,
      );
      continue;
    }

    const heading = line.match(/^(#{1,4})\s+(.*)$/);
    if (heading) {
      blocks.push(<h4 key={`h${i}`} className="mt-4 mb-1.5 font-display text-[15px] font-medium text-ink">{inline(heading[2], `hd${i}`)}</h4>);
      i++;
      continue;
    }

    if (/^\s*([-*]|\d+\.)\s+/.test(line)) {
      const ordered = /^\s*\d+\./.test(line);
      const items: string[] = [];
      while (i < lines.length && /^\s*([-*]|\d+\.)\s+/.test(lines[i])) items.push(lines[i++].replace(/^\s*([-*]|\d+\.)\s+/, ""));
      const List = ordered ? "ol" : "ul";
      blocks.push(
        <List key={`l${i}`} className={`my-2 space-y-1 pl-5 ${ordered ? "list-decimal" : "list-disc"} marker:text-faint`}>
          {items.map((item, j) => <li key={j}>{inline(item, `li${i}${j}`)}</li>)}
        </List>,
      );
      continue;
    }

    const para: string[] = [];
    while (i < lines.length && lines[i].trim() && !isTableRow(lines[i]) && !/^(#{1,4})\s/.test(lines[i]) && !/^\s*([-*]|\d+\.)\s+/.test(lines[i])) para.push(lines[i++]);
    blocks.push(<p key={`p${i}`} className="my-2">{para.map((p, j) => <Fragment key={j}>{j > 0 && <br />}{inline(p, `p${i}${j}`)}</Fragment>)}</p>);
  }
  return <div className="text-[14px] leading-relaxed text-ink/90">{blocks}</div>;
}
