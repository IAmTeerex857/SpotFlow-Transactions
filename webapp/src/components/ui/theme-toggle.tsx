"use client";

import { Moon, Sun } from "lucide-react";
import { useSyncExternalStore } from "react";
import { MorphButton } from "./morph-button";

type Theme = "light" | "dark";

function subscribe(callback: () => void) {
  const observer = new MutationObserver(callback);
  observer.observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });
  return () => observer.disconnect();
}
const getTheme = (): Theme => (document.documentElement.dataset.theme === "dark" ? "dark" : "light");

/** The app theme. Hydrates as "light", then syncs with the saved choice. */
export function useTheme() {
  return useSyncExternalStore(subscribe, getTheme, () => "light" as const);
}

export function setTheme(next: Theme) {
  const root = document.documentElement;
  const apply = () => {
    if (next === "dark") root.dataset.theme = "dark";
    else delete root.dataset.theme;
    try {
      localStorage.setItem("sf-theme", next);
    } catch {}
  };
  const doc = document as Document & { startViewTransition?: (cb: () => void) => unknown };
  if (doc.startViewTransition && !matchMedia("(prefers-reduced-motion: reduce)").matches) doc.startViewTransition(apply);
  else apply();
}

/** Icon button (amicro moon/sun morph). */
export function ThemeToggle() {
  const theme = useTheme();
  const next = theme === "dark" ? "light" : "dark";
  return (
    <MorphButton
      icon={theme === "dark" ? <Moon /> : <Sun />}
      hoverIcon={theme === "dark" ? <Sun /> : <Moon />}
      onClick={() => setTheme(next)}
      variant="ghost"
      size="sm"
      aria-label={`Switch to ${next} mode`}
    />
  );
}
