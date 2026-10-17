import { isStepCount, streamText } from "ai";
import type { NextRequest } from "next/server";
import { z } from "zod";
import { ASK_SYSTEM_PROMPT, askConfigured, contextMessage, ASK_MODEL } from "@/lib/ask/prompt";
import { createAskTools } from "@/lib/ask/tools";
import { getMember } from "@/lib/server/auth";
import { getFilterOptions } from "@/lib/server/report-data";

export const maxDuration = 300;

const Body = z.object({
  messages: z.array(z.object({ role: z.enum(["user", "assistant"]), content: z.string().min(1).max(12_000) })).min(1).max(30),
  view: z.object({ from: z.string(), to: z.string(), filters: z.string().max(400).optional() }).optional(),
});

/** Events streamed to the browser, one JSON object per line. */
export type AskEvent =
  | { type: "thinking"; text: string }
  | { type: "text"; text: string }
  | { type: "tool"; name: string; input: unknown; ok: boolean; ms: number }
  | { type: "done" }
  | { type: "error"; message: string };

export async function POST(request: NextRequest) {
  const { supabase, member } = await getMember();
  if (!member) return Response.json({ error: "Unauthorised" }, { status: 401 });
  if (!askConfigured()) return Response.json({ error: "Ask isn't configured: connect the Vercel AI Gateway." }, { status: 503 });

  const parsed = Body.safeParse(await request.json().catch(() => null));
  if (!parsed.success) return Response.json({ error: "Invalid request" }, { status: 400 });
  const { messages, view } = parsed.data;
  if (messages[messages.length - 1].role !== "user") return Response.json({ error: "The last message must be a question" }, { status: 400 });

  const options = await getFilterOptions(supabase);
  const encoder = new TextEncoder();

  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      const send = (event: AskEvent) => controller.enqueue(encoder.encode(`${JSON.stringify(event)}\n`));
      try {
        const result = streamText({
          model: ASK_MODEL,
          // The fixed prompt comes first so providers can reuse its cached prefix.
          instructions: `${ASK_SYSTEM_PROMPT}\n\n${contextMessage({ minDate: options.minDate, maxDate: options.maxDate, view })}`,
          messages,
          tools: createAskTools(supabase, (e) => send({ type: "tool", ...e })),
          stopWhen: isStepCount(10),
          abortSignal: request.signal,
        });
        for await (const part of result.stream) {
          if (part.type === "text-delta") send({ type: "text", text: part.text });
          else if (part.type === "reasoning-delta") send({ type: "thinking", text: part.text });
          else if (part.type === "error") throw part.error;
        }
        send({ type: "done" });
      } catch (err) {
        console.error("ask failed", err);
        send({ type: "error", message: "The assistant ran into a problem. Please try again." });
      } finally {
        controller.close();
      }
    },
  });

  return new Response(stream, {
    headers: { "content-type": "application/x-ndjson; charset=utf-8", "cache-control": "no-store", "x-accel-buffering": "no" },
  });
}
