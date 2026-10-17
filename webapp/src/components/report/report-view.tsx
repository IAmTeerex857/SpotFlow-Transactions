"use client";

import { AnimatePresence, motion } from "motion/react";
import { Card } from "@/components/ui/card";
import type { Report } from "@/lib/report/build";
import { EntityPanel, FailuresPanel, OverviewPanel, PeaksPanel, RetriesPanel, type Drill } from "./panels";

export type ReportTab = "overview" | "merchants" | "providers" | "regions" | "failures" | "retries" | "peaks";

export function ReportView({ report, tab, onDrill, askBase }: { report: Report; tab: ReportTab; onDrill?: Drill; askBase?: string }) {
  if (!report.totals.total) {
    return (
      <Card className="flex flex-col items-center gap-2 px-6 py-20 text-center">
        <p className="text-[16px] font-medium">No transactions match these filters</p>
        <p className="max-w-md text-[13.5px] text-muted">Try a wider range or clear a filter.</p>
      </Card>
    );
  }
  return (
    <AnimatePresence mode="wait">
      <motion.div key={tab} initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }} transition={{ duration: 0.18 }}>
        {tab === "overview" && <OverviewPanel report={report} />}
        {tab === "merchants" && <EntityPanel report={report} kind="merchant" onDrill={onDrill} askBase={askBase} />}
        {tab === "providers" && <EntityPanel report={report} kind="provider" onDrill={onDrill} askBase={askBase} />}
        {tab === "regions" && <EntityPanel report={report} kind="region" onDrill={onDrill} askBase={askBase} />}
        {tab === "failures" && <FailuresPanel report={report} />}
        {tab === "retries" && <RetriesPanel report={report} />}
        {tab === "peaks" && <PeaksPanel report={report} />}
      </motion.div>
    </AnimatePresence>
  );
}
