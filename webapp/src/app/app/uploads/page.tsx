import type { Metadata } from "next";
import { Upload } from "lucide-react";
import { UploadTable } from "@/components/upload/upload-table";
import { PageHeader } from "@/components/shell/page-header";
import { Card } from "@/components/ui/card";
import { MorphButton } from "@/components/ui/morph-button";
import { requireMember } from "@/lib/server/auth";
import { fmtInt } from "@/lib/format";
import { listUploads } from "@/lib/server/uploads";

export const metadata: Metadata = { title: "Upload history" };

export default async function UploadsPage() {
  const { supabase } = await requireMember();
  const uploads = await listUploads(supabase);
  const total = uploads.filter((u) => u.status === "complete").reduce((s, u) => s + u.parsed_count, 0);
  return (
    <div className="space-y-6">
      <PageHeader
        crumb="Data · History"
        title="Upload history"
        description={`${fmtInt(uploads.length)} uploads · ${fmtInt(total)} transactions parsed`}
        actions={<MorphButton href="/app/upload" icon={<Upload />} variant="primary" size="sm">Upload export</MorphButton>}
      />
      <Card>
        <UploadTable uploads={uploads} />
      </Card>
    </div>
  );
}
