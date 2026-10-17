import type { Metadata } from "next";
import { AppShell } from "@/components/shell/app-shell";

export const metadata: Metadata = { title: "Demo" };

export default function DemoLayout({ children }: LayoutProps<"/demo">) {
  return <AppShell user={null} demo>{children}</AppShell>;
}
