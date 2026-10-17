"use client";

import { RefreshCw, Trash2, Check } from "lucide-react";
import { useRouter } from "next/navigation";
import { useState } from "react";
import { MorphButton } from "@/components/ui/morph-button";

export function UploadActions({ id, canDelete, canReprocess }: { id: string; canDelete: boolean; canReprocess: boolean }) {
  const router = useRouter();
  const [busy, setBusy] = useState<"reprocess" | "delete" | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [done, setDone] = useState(false);

  const reprocess = async () => {
    setBusy("reprocess");
    setMessage(null);
    const res = await fetch(`/api/uploads/${id}/reprocess`, { method: "POST" });
    const json = await res.json().catch(() => ({}));
    setBusy(null);
    if (!res.ok) return setMessage(json.error ?? "Reprocessing failed");
    setDone(true);
    setMessage(`Reprocessed: ${json.stats.transactions.toLocaleString()} transactions`);
    router.refresh();
  };

  const remove = async () => {
    if (!confirm("Delete this upload and all of its transactions? This can't be undone.")) return;
    setBusy("delete");
    const res = await fetch(`/api/uploads/${id}`, { method: "DELETE" });
    const json = await res.json().catch(() => ({}));
    setBusy(null);
    if (!res.ok) return setMessage(json.error ?? "Delete failed");
    router.push("/app/uploads");
    router.refresh();
  };

  return (
    <div className="flex flex-wrap items-center gap-2">
      {message && <span className="text-[12.5px] text-muted">{message}</span>}
      {canReprocess && (
        <MorphButton icon={<RefreshCw />} hoverIcon={done ? <Check /> : undefined} active={done} size="sm" onClick={reprocess} disabled={busy !== null}>
          {busy === "reprocess" ? "Reprocessing…" : "Reprocess"}
        </MorphButton>
      )}
      {canDelete && (
        <MorphButton icon={<Trash2 />} size="sm" onClick={remove} disabled={busy !== null}>
          {busy === "delete" ? "Deleting…" : "Delete"}
        </MorphButton>
      )}
    </div>
  );
}
