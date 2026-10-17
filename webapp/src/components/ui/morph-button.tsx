"use client";

// amicro button interactions: morph (icon swap with a springy pop), slide-arrow (and its vertical twin, drop),
// rotate, shake, glare (a shine sweeps across), text-reveal (label rolls up)
// and magnetic (the button leans toward the cursor).
import { AnimatePresence, motion, useMotionValue, useSpring } from "motion/react";
import Link from "next/link";
import { useRef, useState, type ReactNode } from "react";
import { cn } from "@/lib/utils";

const spring = { type: "spring", stiffness: 600, damping: 25 } as const;

type ButtonVariant = "primary" | "secondary" | "ghost" | "solid";
type Interaction = "morph" | "slide" | "drop" | "rotate" | "shake" | "none";

const variants: Record<ButtonVariant, string> = {
  primary: "bg-accent text-accent-ink hover:bg-accent-hover shadow-[inset_0_1px_0_rgba(255,255,255,.18),0_1px_2px_rgba(16,24,40,.12)]",
  secondary: "bg-raised text-ink shadow-[var(--shadow-card)] hover:bg-sunken",
  ghost: "text-muted hover:text-ink hover:bg-surface-2",
  solid: "bg-ink text-raised hover:opacity-90",
};

const sizes = {
  sm: "h-8 px-3 text-[13px] gap-1.5",
  md: "h-9 px-3.5 text-[13.5px] gap-2",
  lg: "h-11 px-5 text-[14.5px] gap-2.5",
  xl: "h-12 px-6 text-[15px] gap-2.5",
};

interface Props {
  /** Rendered icon element, e.g. <Upload />. Elements (unlike components) can be passed from server components. */
  icon?: ReactNode;
  hoverIcon?: ReactNode;
  /** How the icon reacts on hover. Defaults to "morph" when there is a hoverIcon. */
  interaction?: Interaction;
  /** A light sweep across the button on hover. */
  glare?: boolean;
  /** The label rolls up to a copy of itself on hover. */
  textReveal?: boolean;
  magnetic?: boolean;
  /** Stretch to the container's width. */
  full?: boolean;
  /** Icon after the label instead of before it. */
  trailing?: boolean;
  /** Extra classes for the icon slot (e.g. a white circle behind it). */
  iconClassName?: string;
  children?: ReactNode;
  variant?: ButtonVariant;
  size?: keyof typeof sizes;
  shape?: "rounded" | "pill";
  className?: string;
  href?: string;
  type?: "button" | "submit";
  disabled?: boolean;
  /** Keep the hover icon shown (e.g. after a successful action). */
  active?: boolean;
  onClick?: () => void;
  "aria-label"?: string;
}

function IconSlot({ icon, hoverIcon, interaction, hovered, className }: {
  icon: ReactNode; hoverIcon?: ReactNode; interaction: Interaction; hovered: boolean; className?: string;
}) {
  const box = cn("relative flex size-4 shrink-0 items-center justify-center overflow-visible [&_svg]:size-4", className);
  if (interaction === "rotate") {
    return <motion.span className={box} animate={{ rotate: hovered ? 180 : 0 }} transition={{ type: "spring", stiffness: 260, damping: 18 }}>{icon}</motion.span>;
  }
  if (interaction === "shake") {
    return <motion.span className={box} animate={hovered ? { rotate: [0, -14, 12, -8, 6, 0] } : { rotate: 0 }} transition={{ duration: 0.5 }}>{icon}</motion.span>;
  }
  if ((interaction === "slide" || interaction === "drop") && hoverIcon) {
    // slide: out to the right, in from the left. drop: out the bottom, in from the top.
    const axis = interaction === "slide" ? "x" : "y";
    return (
      <span className={cn(box, "overflow-hidden")}>
        <motion.span className="absolute inset-0 flex items-center justify-center" animate={{ [axis]: hovered ? 18 : 0, opacity: hovered ? 0 : 1 }} transition={spring}>{icon}</motion.span>
        <motion.span className="absolute inset-0 flex items-center justify-center" initial={false} animate={{ [axis]: hovered ? 0 : -18, opacity: hovered ? 1 : 0 }} transition={spring}>{hoverIcon}</motion.span>
      </span>
    );
  }
  const swapped = hovered && hoverIcon && interaction === "morph";
  return (
    <span className={box}>
      <AnimatePresence mode="popLayout" initial={false}>
        <motion.span
          key={swapped ? "b" : "a"}
          initial={{ scale: 0.5, opacity: 0 }}
          animate={{ scale: 1, opacity: 1 }}
          exit={{ scale: 0.5, opacity: 0 }}
          transition={spring}
          className="absolute inset-0 flex items-center justify-center"
        >
          {swapped ? hoverIcon : icon}
        </motion.span>
      </AnimatePresence>
    </span>
  );
}

export function MorphButton({
  icon, hoverIcon, interaction, glare, textReveal, magnetic, full, trailing, iconClassName, children,
  variant = "secondary", size = "md", shape = "rounded", className, href, type = "button", disabled, active, onClick, ...rest
}: Props) {
  const [hovered, setHovered] = useState(false);
  const ref = useRef<HTMLSpanElement>(null);
  const mx = useSpring(useMotionValue(0), { stiffness: 300, damping: 20 });
  const my = useSpring(useMotionValue(0), { stiffness: 300, damping: 20 });
  const mode: Interaction = interaction ?? (hoverIcon ? "morph" : "none");
  const on = hovered || !!active;

  const iconNode = icon && <IconSlot icon={icon} hoverIcon={hoverIcon} interaction={mode} hovered={on} className={iconClassName} />;
  const label = children && (
    textReveal ? (
      <span className="relative block overflow-hidden font-medium tracking-[-0.01em] whitespace-nowrap">
        <motion.span className="block" animate={{ y: hovered ? "-100%" : "0%" }} transition={{ type: "spring", stiffness: 400, damping: 30 }}>{children}</motion.span>
        <motion.span aria-hidden className="absolute inset-0 block" animate={{ y: hovered ? "0%" : "100%" }} transition={{ type: "spring", stiffness: 400, damping: 30 }}>{children}</motion.span>
      </span>
    ) : <span className="font-medium tracking-[-0.01em] whitespace-nowrap">{children}</span>
  );
  const content = (
    <>
      {glare && (
        <span aria-hidden className="pointer-events-none absolute inset-0 overflow-hidden rounded-[inherit]">
          <motion.span
            className="absolute inset-y-0 -left-1/2 w-1/2 -skew-x-12 bg-gradient-to-r from-transparent via-white/45 to-transparent"
            initial={false}
            animate={{ x: hovered ? "320%" : "0%" }}
            transition={hovered ? { duration: 0.75, ease: [0.2, 0.7, 0.2, 1] } : { duration: 0 }}
          />
        </span>
      )}
      {!trailing && iconNode}
      {label}
      {trailing && iconNode}
    </>
  );
  const classes = cn(
    "relative inline-flex cursor-pointer select-none items-center justify-center transition-[background-color,opacity,box-shadow] duration-150 disabled:pointer-events-none disabled:opacity-50",
    shape === "pill" ? "rounded-full" : "rounded-[10px]",
    variants[variant], sizes[size], !children && "aspect-square px-0", full && "w-full", className,
  );
  const motionProps = {
    onHoverStart: () => setHovered(true),
    onHoverEnd: () => { setHovered(false); mx.set(0); my.set(0); },
    whileTap: { scale: 0.97 },
    whileHover: { scale: 1.02 },
  };
  const onPointerMove = magnetic
    ? (e: React.PointerEvent) => {
        const r = ref.current?.getBoundingClientRect();
        if (!r) return;
        mx.set((e.clientX - r.left - r.width / 2) * 0.22);
        my.set((e.clientY - r.top - r.height / 2) * 0.3);
      }
    : undefined;

  if (href) {
    // API routes (downloads) need a full request, not client-side navigation.
    const plain = href.startsWith("/api/") || href.startsWith("http");
    return (
      <motion.span ref={ref} {...motionProps} onPointerMove={onPointerMove} style={magnetic ? { x: mx, y: my } : undefined} className={full ? "flex w-full" : "inline-flex"}>
        {plain ? (
          <a href={href} className={classes} aria-label={rest["aria-label"]}>{content}</a>
        ) : (
          <Link href={href} className={classes} aria-label={rest["aria-label"]}>{content}</Link>
        )}
      </motion.span>
    );
  }
  return (
    <motion.span ref={ref} onPointerMove={onPointerMove} style={magnetic ? { x: mx, y: my } : undefined} className={full ? "flex w-full" : "inline-flex"}>
      <motion.button {...motionProps} type={type} disabled={disabled} onClick={onClick} className={classes} aria-label={rest["aria-label"]}>
        {content}
      </motion.button>
    </motion.span>
  );
}
