import type { Metadata } from "next";
import { UploadFlow } from "@/components/upload/upload-flow";
import { requireMember } from "@/lib/server/auth";
import { getRegistryExtras } from "@/lib/server/registry";

export const metadata: Metadata = { title: "Upload" };

export default async function UploadPage() {
  const { supabase } = await requireMember();
  const extras = await getRegistryExtras(supabase);
  return <UploadFlow extras={extras} />;
}
