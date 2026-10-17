import type { Metadata } from "next";
import { Download, MessageSquareText } from "lucide-react";
import { redirect } from "next/navigation";
import { LiveExplorer } from "@/components/report/live-explorer";
import { MorphButton } from "@/components/ui/morph-button";
import { requireMember } from "@/lib/server/auth";
import { getFilterOptions, getReport } from "@/lib/server/report-data";
import { filtersToQuery, parseFilters } from "@/lib/report/filters";

export const metadata: Metadata = { title: "Reports" };

export default async function ReportsPage({ searchParams }: PageProps<"/app/reports">) {
  const { supabase } = await requireMember();
  const options = await getFilterOptions(supabase);
  if (!options.maxDate) redirect("/app");
  const filters = parseFilters(await searchParams, options);
  const report = await getReport(supabase, filters);
  const query = filtersToQuery(filters);
  return (
    <LiveExplorer
      report={report}
      filters={filters}
      options={options}
      basePath="/app/reports"
      actions={
        <>
          <MorphButton href={`/app/ask?from=${filters.from}&to=${filters.to}`} icon={<MessageSquareText />} size="sm">Ask about this</MorphButton>
          <MorphButton href={`/api/report/export?${query}`} icon={<Download />} size="sm" variant="solid">Export</MorphButton>
        </>
      }
    />
  );
}
