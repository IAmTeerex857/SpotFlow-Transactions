import { createBrowserClient } from "@supabase/ssr";
import { supabaseEnv } from "./env";

export function createClient() {
  const env = supabaseEnv();
  if (!env) throw new Error("Supabase is not configured");
  return createBrowserClient(env.url, env.key);
}
