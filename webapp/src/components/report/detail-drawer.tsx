"use client";

import { AnimatePresence, motion } from "motion/react";
import { X } from "lucide-react";
import { useEffect } from "react";

/** Right-hand details panel (workflow-builder reference). */
export function DetailDrawer({ open, onClose, eyebrow, title, actions, children }: {
  open: boolean; onClose: () => void; eyebrow?: string; title: React.ReactNode; actions?: React.ReactNode; children: React.ReactNode;
}) {
  useEffect(() => {
    if (!open) return;
    const esc = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    document.addEventListener("keydown", esc);
    return () => document.removeEventListener("keydown", esc);
  }, [open, onClose]);
  return (
    <AnimatePresence>
      {open && (
        <>
          <motion.div className="fixed inset-0 z-40 bg-black/20" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} onClick={onClose} />
          <motion.aside
            role="dialog"
            aria-label={typeof title === "string" ? title : "Details"}
            className="fixed inset-y-2 right-2 z-50 flex w-[min(460px,calc(100vw-1rem))] flex-col overflow-hidden rounded-2xl bg-raised shadow-[var(--shadow-pop)]"
            initial={{ x: 480 }}
            animate={{ x: 0 }}
            exit={{ x: 480 }}
            transition={{ type: "spring", stiffness: 380, damping: 40 }}
          >
            <div className="flex items-start justify-between gap-3 border-b border-line px-5 py-4">
              <div className="min-w-0">
                {eyebrow && <div className="mb-1 text-[12px] text-faint">{eyebrow}</div>}
                <h2 className="truncate text-[17px] font-semibold tracking-[-0.02em]">{title}</h2>
              </div>
              <button onClick={onClose} className="cursor-pointer rounded-lg p-1.5 text-faint hover:bg-surface-2 hover:text-ink" aria-label="Close"><X className="size-4" /></button>
            </div>
            <div className="flex-1 space-y-6 overflow-y-auto px-5 py-5 scroll-quiet">{children}</div>
            {actions && <div className="flex items-center justify-end gap-2 border-t border-line px-5 py-3.5">{actions}</div>}
          </motion.aside>
        </>
      )}
    </AnimatePresence>
  );
}
