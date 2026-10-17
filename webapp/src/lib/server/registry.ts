import "server-only";
import type { SupabaseClient } from "@supabase/supabase-js";
import { buildRegistry, type Registry } from "@/lib/parser/registry";

interface RegistryExtras {
  providers: string[];
  regions: string[];
}

export async function getRegistryExtras(supabase: SupabaseClient): Promise<RegistryExtras> {
  const { data, error } = await supabase.from("registry_entries").select("kind, name");
  if (error) throw new Error(`Could not load the provider registry: ${error.message}`);
  return {
    providers: data.filter((r) => r.kind === "provider").map((r) => r.name),
    regions: data.filter((r) => r.kind === "region").map((r) => r.name),
  };
}

export async function getRegistry(supabase: SupabaseClient): Promise<Registry> {
  const extras = await getRegistryExtras(supabase);
  return buildRegistry({ extraProviders: extras.providers, extraRegions: extras.regions });
}
