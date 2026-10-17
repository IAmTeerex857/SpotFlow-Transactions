"use client";

import { LogIn } from "lucide-react";
import { useMemo, useState } from "react";
import { MorphButton } from "@/components/ui/morph-button";
import { sampleTransactions } from "@/lib/demo/sample";
import { buildReport } from "@/lib/report/build";
import { cubeFromTransactions } from "@/lib/report/cube";
import { presetRange, type FilterOptions } from "@/lib/report/filters";
import type { ReportFilters } from "@/lib/report/types";
import { ReportExplorer } from "./report-explorer";

const uniq = (values: string[]) => [...new Set(values)].sort();

/** The full report UI over synthetic data, filtered in the browser. */
export function DemoExplorer() {
  const rows = useMemo(() => sampleTransactions(), []);
  const options: FilterOptions = useMemo(() => {
    const dates = rows.map((r) => r.businessDate).sort();
    return {
      merchants: uniq(rows.map((r) => r.merchant)),
      providers: uniq(rows.map((r) => r.provider)),
      regions: uniq(rows.map((r) => r.region)),
      channels: uniq(rows.map((r) => r.channel)),
      minDate: dates[0],
      maxDate: dates[dates.length - 1],
    };
  }, [rows]);
  const [filters, setFilters] = useState<ReportFilters>(() => ({ ...presetRange("30d", options) }));

  const report = useMemo(() => {
    const has = (list: string[] | undefined, v: string) => !list?.length || list.includes(v);
    return buildReport(cubeFromTransactions(rows.filter((r) =>
      r.businessDate >= filters.from && r.businessDate <= filters.to &&
      has(filters.merchants, r.merchant) && has(filters.providers, r.provider) &&
      has(filters.regions, r.region) && has(filters.channels, r.channel),
    )));
  }, [rows, filters]);

  return (
    <ReportExplorer
      report={report}
      filters={filters}
      options={options}
      onChange={setFilters}
      crumb="Reports · sample data"
      actions={<MorphButton href="/login" icon={<LogIn />} variant="primary" size="sm">Sign in to use your data</MorphButton>}
    />
  );
}
