"use client";

import { Check } from "lucide-react";
import { fmtRange } from "@/lib/format";
import { detectPreset, presetRange, type FilterOptions, type Preset } from "@/lib/report/filters";
import { cn } from "@/lib/utils";
import { Popover, SegmentTrigger } from "@/components/ui/popover";

const PRESETS: { value: Exclude<Preset, "custom">; label: string }[] = [
  { value: "latest", label: "Latest day" },
  { value: "7d", label: "Last 7 days" },
  { value: "30d", label: "Last 30 days" },
  { value: "month", label: "Month to date" },
  { value: "all", label: "All time" },
];

export function RangeSelect({ from, to, options, onChange }: {
  from: string; to: string; options: FilterOptions; onChange: (range: { from: string; to: string }) => void;
}) {
  const preset = detectPreset({ from, to }, options);
  const value = preset === "custom" ? fmtRange(from, to) : PRESETS.find((p) => p.value === preset)!.label;
  return (
    <Popover className="w-72" trigger={({ open, toggle }) => <SegmentTrigger label="Range" value={value} open={open} onClick={toggle} />}>
      {(close) => (
        <div className="p-1.5">
          {PRESETS.map((p) => (
            <button
              key={p.value}
              onClick={() => { onChange(presetRange(p.value, options)); close(); }}
              className="flex w-full cursor-pointer items-center justify-between rounded-lg px-2.5 py-2 text-left text-[13px] text-ink hover:bg-surface-2"
            >
              {p.label}
              {preset === p.value && <Check className="size-3.5 text-accent" />}
            </button>
          ))}
          <div className="mt-1.5 border-t border-line px-2.5 pb-1.5 pt-3">
            <div className="mb-2 text-[11.5px] text-faint">Custom range</div>
            <div className="grid grid-cols-2 gap-2">
              {(["from", "to"] as const).map((k) => (
                <input
                  key={k}
                  type="date"
                  aria-label={k === "from" ? "From date" : "To date"}
                  value={k === "from" ? from : to}
                  min={k === "from" ? options.minDate ?? undefined : from}
                  max={k === "from" ? to : options.maxDate ?? undefined}
                  onChange={(e) => e.target.value && onChange(k === "from" ? { from: e.target.value, to } : { from, to: e.target.value })}
                  className={cn("h-8 rounded-lg border border-line bg-raised px-2 text-[12.5px] text-ink outline-none focus:border-accent [color-scheme:inherit]")}
                />
              ))}
            </div>
          </div>
        </div>
      )}
    </Popover>
  );
}
