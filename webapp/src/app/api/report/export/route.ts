import { NextResponse, type NextRequest } from "next/server";
import { getMember } from "@/lib/server/auth";
import { getFilterOptions, getReport } from "@/lib/server/report-data";
import { renderReportHtml } from "@/lib/report/export-html";
import { parseFilters } from "@/lib/report/filters";

export async function GET(request: NextRequest) {
  const { supabase, member } = await getMember();
  if (!member) return NextResponse.json({ error: "Unauthorised" }, { status: 401 });
  const options = await getFilterOptions(supabase);
  const filters = parseFilters(Object.fromEntries(request.nextUrl.searchParams), options);
  const report = await getReport(supabase, filters);
  const html = renderReportHtml(report, filters, { generatedBy: member.email, generatedAt: new Date().toUTCString() });
  return new NextResponse(html, {
    headers: {
      "content-type": "text/html; charset=utf-8",
      "content-disposition": `attachment; filename="spotflow-insights-${filters.from}-to-${filters.to}.html"`,
      "cache-control": "private, no-store",
    },
  });
}
