import { LogIn } from "lucide-react";
import { PageHeader } from "@/components/shell/page-header";
import { HomeView } from "@/components/home/home-view";
import { MorphButton } from "@/components/ui/morph-button";
import { SAMPLE_UPLOADS, sampleHome } from "@/lib/demo/home";
import { fmtDate, fmtInt } from "@/lib/format";

export default function DemoHomePage() {
  const summary = sampleHome();
  return (
    <div className="space-y-7">
      <PageHeader
        crumb="Home · sample data"
        title="Good morning, Ada"
        description={`Latest export · ${summary.latestDate ? fmtDate(summary.latestDate, { weekday: "short", month: "short", day: "numeric" }) : "—"} · ${fmtInt(summary.latest.totals.total)} transactions · fictional merchants`}
        actions={<MorphButton href="/login" icon={<LogIn />} variant="primary" size="sm">Sign in to use your data</MorphButton>}
      />
      <HomeView summary={summary} uploads={SAMPLE_UPLOADS} base="/demo" demo />
    </div>
  );
}
