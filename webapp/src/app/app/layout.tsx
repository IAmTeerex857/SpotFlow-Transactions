import type { Metadata } from "next";
import { AppShell } from "@/components/shell/app-shell";
import { requireMember } from "@/lib/server/auth";

export const metadata: Metadata = { title: "Home" };

export default async function AppLayout({ children }: LayoutProps<"/app">) {
  const { supabase, member } = await requireMember();
  const { data } = await supabase
    .from("uploads")
    .select("file_name, business_date, parsed_count, created_at")
    .eq("status", "complete")
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();
  return (
    <AppShell
      user={{ email: member.email, name: member.name, avatarUrl: member.avatarUrl }}
      latest={data ? { fileName: data.file_name, businessDate: data.business_date, transactions: data.parsed_count, createdAt: data.created_at } : null}
    >
      {children}
    </AppShell>
  );
}
