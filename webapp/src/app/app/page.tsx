import { Download, FileUp, Upload } from "lucide-react";
import { PageHeader } from "@/components/shell/page-header";
import { HomeView } from "@/components/home/home-view";
import { Card } from "@/components/ui/card";
import { MorphButton } from "@/components/ui/morph-button";
import { requireMember } from "@/lib/server/auth";
import { getCube, getFilterOptions } from "@/lib/server/report-data";
import { fmtDate, fmtInt } from "@/lib/format";
import { greeting } from "@/lib/format";
import { addDays } from "@/lib/report/filters";
import { buildHome } from "@/lib/report/home";
import { listUploads } from "@/lib/server/uploads";

export default async function HomePage() {
  const { supabase, member } = await requireMember();
  const options = await getFilterOptions(supabase);
  const title = greeting(member.name);

  if (!options.maxDate) {
    return (
      <div className="space-y-6">
        <PageHeader crumb="Home" title={title} description="Upload your first export to see the dashboard." />
        <Card className="flex flex-col items-center gap-4 px-8 py-16 text-center">
          <span className="flex size-12 items-center justify-center rounded-2xl bg-accent-dim text-accent"><FileUp className="size-6" /></span>
          <h2 className="text-[18px] font-semibold tracking-[-0.02em]">No exports yet</h2>
          <p className="max-w-md text-[13.5px] text-muted">Raw exports are fine: customer emails are pseudonymised and phone numbers masked before anything is stored.</p>
          <MorphButton href="/app/upload" icon={<Upload />} variant="primary">Upload an export</MorphButton>
        </Card>
      </div>
    );
  }

  const latest = options.maxDate;
  const [trendCube, latestCube, uploads] = await Promise.all([
    getCube(supabase, { from: addDays(latest, -29), to: latest }),
    getCube(supabase, { from: latest, to: latest }),
    listUploads(supabase, 5),
  ]);
  const summary = buildHome(trendCube, latestCube);

  return (
    <div className="space-y-7">
      <PageHeader
        crumb="Home"
        title={title}
        description={`Latest export · ${fmtDate(latest, { weekday: "short", month: "short", day: "numeric" })} · ${fmtInt(summary.latest.totals.total)} transactions`}
        actions={
          <>
            <MorphButton href={`/api/report/export?from=${latest}&to=${latest}`} icon={<Download />} size="sm">Export</MorphButton>
            <MorphButton href="/app/upload" icon={<Upload />} variant="primary" size="sm">Upload export</MorphButton>
          </>
        }
      />
      <HomeView
        summary={summary}
        base="/app"
        uploads={uploads.map((u) => ({ id: u.id, fileName: u.file_name, status: u.status, transactions: u.parsed_count, durationMs: u.duration_ms, createdAt: u.created_at }))}
      />
    </div>
  );
}
