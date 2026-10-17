import type { Metadata } from "next";
import { AskChat } from "@/components/ask/ask-chat";
import { askConfigured } from "@/lib/ask/prompt";
import { requireMember } from "@/lib/server/auth";

export const metadata: Metadata = { title: "Ask" };

export default async function AskPage({ searchParams }: PageProps<"/app/ask">) {
  await requireMember();
  const q = await searchParams;
  const from = typeof q.from === "string" ? q.from : undefined;
  const to = typeof q.to === "string" ? q.to : undefined;
  const question = typeof q.q === "string" ? q.q.slice(0, 500) : undefined;
  return <AskChat configured={askConfigured()} view={from && to ? { from, to } : undefined} initialQuestion={question} />;
}
