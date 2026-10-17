"use client";

import { Check, Search } from "lucide-react";
import { useMemo, useState } from "react";
import { cn } from "@/lib/utils";
import { Popover, SegmentTrigger } from "@/components/ui/popover";

/** Multi-select filter with search, behind a "Label | value ▾" trigger. */
export function FilterSelect({ label, options, selected, onChange, format = (v) => v, allLabel = "All" }: {
  label: string;
  options: string[];
  selected: string[];
  onChange: (values: string[]) => void;
  format?: (value: string) => string;
  allLabel?: string;
}) {
  const [query, setQuery] = useState("");
  const filtered = useMemo(
    () => options.filter((o) => format(o).toLowerCase().includes(query.trim().toLowerCase())),
    [options, query, format],
  );
  const summary = selected.length === 0 ? allLabel : selected.length === 1 ? format(selected[0]) : `${selected.length} selected`;
  const toggle = (value: string) => onChange(selected.includes(value) ? selected.filter((v) => v !== value) : [...selected, value]);

  return (
    <Popover
      className="w-64"
      trigger={({ open, toggle: t }) => <SegmentTrigger label={label} value={summary} open={open} onClick={t} active={selected.length > 0} />}
    >
      {() => (
        <>
          {options.length > 7 && (
            <label className="flex items-center gap-2 border-b border-line px-3 py-2.5">
              <Search className="size-3.5 text-faint" />
              <input autoFocus value={query} onChange={(e) => setQuery(e.target.value)} placeholder={`Search ${label.toLowerCase()}`} className="w-full bg-transparent text-[13px] text-ink outline-none placeholder:text-faint" />
            </label>
          )}
          <div className="px-3 pb-1 pt-2.5 text-[11.5px] text-faint">{label}</div>
          <ul role="listbox" aria-multiselectable className="max-h-72 overflow-y-auto px-1.5 pb-1.5 scroll-quiet">
            <li>
              <button onClick={() => onChange([])} className="flex w-full cursor-pointer items-center gap-2.5 rounded-lg px-2.5 py-2 text-left text-[13px] text-ink hover:bg-surface-2">
                <span className="flex size-4 items-center justify-center">{selected.length === 0 && <span className="size-1.5 rounded-full bg-accent" />}</span>
                {allLabel}
              </button>
            </li>
            {filtered.map((option) => {
              const on = selected.includes(option);
              return (
                <li key={option}>
                  <button role="option" aria-selected={on} onClick={() => toggle(option)} className="flex w-full cursor-pointer items-center gap-2.5 rounded-lg px-2.5 py-2 text-left text-[13px] text-ink hover:bg-surface-2">
                    <span className={cn("flex size-4 items-center justify-center rounded-[5px] border transition-colors", on ? "border-accent bg-accent text-white" : "border-line")}>
                      {on && <Check className="size-3" strokeWidth={3} />}
                    </span>
                    <span className="truncate">{format(option)}</span>
                  </button>
                </li>
              );
            })}
            {!filtered.length && <li className="px-3 py-4 text-center text-[12.5px] text-faint">No matches</li>}
          </ul>
        </>
      )}
    </Popover>
  );
}
