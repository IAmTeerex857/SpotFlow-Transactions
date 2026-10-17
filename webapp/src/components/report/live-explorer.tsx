"use client";

import { useRouter } from "next/navigation";
import { useTransition } from "react";
import type { Report } from "@/lib/report/build";
import { filtersToQuery, type FilterOptions } from "@/lib/report/filters";
import type { ReportFilters } from "@/lib/report/types";
import { ReportExplorer } from "./report-explorer";

/** Filters live in the URL so any view can be shared as a link. */
export function LiveExplorer(props: {
  report: Report;
  filters: ReportFilters;
  options: FilterOptions;
  basePath: string;
  title?: string;
  crumb?: string;
  actions?: React.ReactNode;
  banner?: React.ReactNode;
}) {
  const router = useRouter();
  const [pending, startTransition] = useTransition();
  const { basePath, ...rest } = props;
  return (
    <ReportExplorer
      {...rest}
      pending={pending}
      askBase="/app/ask"
      onChange={(next) => startTransition(() => router.push(`${basePath}?${filtersToQuery(next)}${window.location.hash}`, { scroll: false }))}
    />
  );
}
