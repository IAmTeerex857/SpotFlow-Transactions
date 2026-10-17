"use client";

// Chatsheet-style isometric hero, built from DOM on one CSS-3D plane: export
// tiles ride dashed paths into the Spotflow disc and report cards slide out
// along a rail. Everything is in-plane, so text and icons stay crisp, and all
// motion is CSS (transform / offset-path), paused when off screen.
import { motion, useMotionTemplate, useReducedMotion, useSpring } from "motion/react";
import {
  Banknote, CreditCard, FileSpreadsheet, Globe2, Landmark, QrCode, Receipt, Smartphone, Wallet, type LucideIcon,
} from "lucide-react";
import { useEffect, useRef, type CSSProperties } from "react";
import { MARK } from "@/components/brand/logo";

/** Stacked hard shadows offset toward screen-down in plane space: a solid side. */
function extrude(depth: number, side: string, edge: string, soft = "rgba(36, 56, 120, 0.14)") {
  const layers = Array.from({ length: depth }, (_, i) => `${-(i + 1)}px ${i + 1}px 0 ${i === depth - 1 ? edge : side}`);
  layers.push(`${-depth - 14}px ${depth + 14}px 28px ${soft}`);
  return layers.join(", ");
}

const TILE_SHADOW = extrude(7, "#e1e7f7", "#c9d3ef");
const CARD_SHADOW = extrude(9, "#e3e9f8", "#cbd5f0", "rgba(36, 56, 120, 0.10)");
const PILL_SHADOW = extrude(6, "#2a55e0", "#1f45c4", "rgba(51, 102, 255, 0.25)");
const DISC_SHADOW = extrude(40, "#111a3b", "#0b1230", "rgba(20, 30, 80, 0.22)");

// Plane coordinates: origin at the disc centre, +x runs up-right on screen, +y down-right.
const PATHS = {
  main: "M -622 -792 C -600 -600, -620 -480, -580 -411 C -520 -330, -380 -420, -311 -367 C -250 -320, -340 -170, -311 -113 C -280 -60, -220 -70, -150 -60",
  low: "M -1060 -60 C -860 -20, -720 22, -622 28 C -520 36, -420 44, -325 42 C -250 40, -190 55, -140 60",
  front: "M -600 520 C -460 400, -320 280, -196 196",
};

interface TileDef { icon: LucideIcon; color: string }
const MAIN_TILES: TileDef[] = [
  { icon: FileSpreadsheet, color: "#1f9d55" },
  { icon: CreditCard, color: "#3366ff" },
  { icon: Smartphone, color: "#e8a20c" },
  { icon: Receipt, color: "#f2735f" },
  { icon: Landmark, color: "#7457e0" },
  { icon: Wallet, color: "#009688" },
  { icon: QrCode, color: "#1c2752" },
];
const LOW_TILES: TileDef[] = [{ icon: Banknote, color: "#1f9d55" }, { icon: Globe2, color: "#3366ff" }];
const FRONT_TILES: TileDef[] = [{ icon: FileSpreadsheet, color: "#1f9d55" }, { icon: CreditCard, color: "#7457e0" }];

function TileFace({ def, size = 66 }: { def: TileDef; size?: number }) {
  return (
    <span
      className="flex items-center justify-center rounded-[16px] border border-[#e6ebf7] bg-white"
      style={{ width: size, height: size, boxShadow: TILE_SHADOW }}
    >
      <def.icon style={{ color: def.color, width: size * 0.44, height: size * 0.44 }} strokeWidth={2} />
    </span>
  );
}

function PathTiles({ tiles, path, duration, rest }: { tiles: TileDef[]; path: string; duration: number; rest: number[] }) {
  const gap = duration / tiles.length;
  return tiles.map((def, i) => (
    <span
      key={i}
      className="hx-tile absolute left-0 top-0"
      style={{
        offsetPath: `path("${path}")`,
        offsetRotate: "0deg",
        offsetDistance: `${rest[i] ?? 50}%`,
        animationDuration: `${duration}s`,
        animationDelay: `${-i * gap}s`,
      } as CSSProperties}
    >
      <TileFace def={def} />
    </span>
  ));
}

const CARDS: { label: string; body: React.ReactNode }[] = [
  {
    label: "Failure reasons",
    body: (
      <div className="space-y-3.5">
        {[["#d93a3f", "78%"], ["#e8a20c", "52%"], ["#7457e0", "34%"]].map(([c, w]) => (
          <div key={c} className="flex items-center gap-2.5">
            <span className="size-3 rounded-full" style={{ background: c }} />
            <span className="h-2.5 rounded-full" style={{ width: w, background: `${c}55` }} />
          </div>
        ))}
      </div>
    ),
  },
  {
    label: "Provider health",
    body: (
      <div className="grid grid-cols-3 gap-2.5">
        {["#009688", "#009688", "#d93a3f", "#009688", "#e8a20c", "#009688"].map((c, i) => (
          <span key={i} className="flex h-9 items-center justify-center rounded-lg border border-[#e6ebf7] bg-[#f7f9fe]">
            <span className="size-2.5 rounded-full" style={{ background: c }} />
          </span>
        ))}
      </div>
    ),
  },
  {
    label: "Daily summary",
    body: (
      <div className="flex h-[86px] items-end gap-2">
        {[40, 62, 48, 80, 56, 92, 70].map((h, i) => (
          <span key={i} className="flex-1 rounded-t-md" style={{ height: `${h}%`, background: i === 5 ? "#3366ff" : "#c9d6ff" }} />
        ))}
      </div>
    ),
  },
  {
    label: "Retries",
    body: (
      <div className="flex items-center justify-between pt-4">
        {["#d93a3f", "#e8a20c", "#009688"].map((c, i) => (
          <span key={c} className="flex items-center gap-2">
            <span className="flex size-10 items-center justify-center rounded-xl border border-[#e6ebf7] bg-white">
              <span className="size-3.5 rounded-full" style={{ background: c }} />
            </span>
            {i < 2 && <span className="h-0 w-8 border-t-2 border-dashed border-[#b7c6f5]" />}
          </span>
        ))}
      </div>
    ),
  },
  {
    label: "Regions",
    body: (
      <div className="space-y-2.5">
        {[88, 64, 72, 40].map((w, i) => (
          <div key={i} className="flex items-center gap-2">
            <span className="h-2 w-8 rounded-full bg-[#dfe6f9]" />
            <span className="h-2 rounded-full bg-[#3366ff]" style={{ width: `${w * 0.7}%`, opacity: 0.35 + i * 0.15 }} />
          </div>
        ))}
      </div>
    ),
  },
];

const CARD_DURATION = 26;

function ReportCard({ index, label, children }: { index: number; label: string; children: React.ReactNode }) {
  const gap = CARD_DURATION / CARDS.length;
  return (
    <div
      className="hx-card absolute left-0 top-[-72px] h-[200px] w-[300px]"
      style={{ animationDuration: `${CARD_DURATION}s`, animationDelay: `${-index * gap}s`, "--rest": `${120 + index * 372}px` } as CSSProperties}
    >
      <div className="relative h-full w-full rounded-[22px] border border-[#e4e9f7] bg-white p-6" style={{ boxShadow: CARD_SHADOW }}>
        <div className="mb-5 flex items-center gap-2">
          <span className="h-2 w-16 rounded-full bg-[#e3e9f8]" />
          <span className="h-2 w-8 rounded-full bg-[#eef2fb]" />
        </div>
        {children}
        <span
          className="absolute -bottom-5 right-[-26px] flex h-11 items-center whitespace-nowrap rounded-full bg-[#3366ff] px-6 text-[17px] font-medium tracking-[-0.01em] text-white"
          style={{ boxShadow: PILL_SHADOW }}
        >
          {label}
        </span>
      </div>
    </div>
  );
}

function Disc() {
  return (
    <div className="absolute left-[-150px] top-[-150px] size-[300px]">
      <div className="hx-ripple absolute inset-0 rounded-full" />
      <div className="hx-ripple absolute inset-0 rounded-full" style={{ animationDelay: "-1.6s" }} />
      <div className="absolute inset-0 rounded-full bg-white" style={{ boxShadow: DISC_SHADOW }} />
      <svg viewBox="-150 -150 300 300" className="absolute inset-0 size-full" aria-hidden>
        <defs>
          <radialGradient id="hx-face" cx="0.55" cy="0.45" r="0.7">
            <stop offset="0" stopColor="#dbe4fd" />
            <stop offset="1" stopColor="#bccbf8" />
          </radialGradient>
        </defs>
        <circle r="138" fill="url(#hx-face)" />
        <g className="hx-spin">
          <circle r="108" fill="none" stroke="#3d6bff" strokeWidth="18" strokeDasharray="300 39.3" strokeLinecap="round" />
        </g>
        <g className="hx-spin-rev">
          <circle r="70" fill="none" stroke="#3d6bff" strokeWidth="18" strokeDasharray="190 30 170 50" strokeLinecap="round" />
        </g>
        <circle r="38" fill="#3366ff" />
        <svg x="-19" y="-18" width="38" height="36" viewBox="0 0 25.7815 24.0069" fill="#ffffff">
          {MARK.map((d) => <path key={d.slice(0, 12)} d={d} />)}
        </svg>
      </svg>
    </div>
  );
}

export function HeroScene() {
  const root = useRef<HTMLDivElement>(null);
  const reduced = useReducedMotion();
  const rx = useSpring(60, { stiffness: 60, damping: 18 });
  const rz = useSpring(-45, { stiffness: 60, damping: 18 });
  const transform = useMotionTemplate`rotateX(${rx}deg) rotateZ(${rz}deg)`;

  useEffect(() => {
    const el = root.current;
    if (!el) return;
    // Pause every animation while the hero is off screen.
    const io = new IntersectionObserver(([e]) => el.toggleAttribute("data-paused", !e.isIntersecting), { rootMargin: "80px" });
    io.observe(el);
    if (reduced || !matchMedia("(pointer: fine)").matches) return () => io.disconnect();
    // Gentle parallax: the plane tilts a few degrees toward the cursor.
    const onMove = (e: PointerEvent) => {
      rz.set(-45 + (e.clientX / innerWidth - 0.5) * 6);
      rx.set(60 - (e.clientY / innerHeight - 0.5) * 5);
    };
    addEventListener("pointermove", onMove, { passive: true });
    return () => {
      io.disconnect();
      removeEventListener("pointermove", onMove);
    };
  }, [reduced, rx, rz]);

  return (
    <div ref={root} className="hx pointer-events-none absolute inset-0 select-none" aria-hidden>
      <div className="hx-stage absolute size-0">
        <motion.div className="absolute left-0 top-0 size-0 origin-top-left" style={{ transform }}>
          {/* ground: halo, rail and dashed paths */}
          <div className="absolute left-[-460px] top-[-460px] size-[920px] rounded-full bg-[radial-gradient(closest-side,rgba(205,218,252,0.7),rgba(205,218,252,0))]" />
          <div className="hx-rail absolute left-[60px] top-[-104px] h-[264px] w-[2100px] rounded-[34px]" />
          <svg className="absolute left-[-1600px] top-[-1000px] overflow-visible" width="3200" height="2000" viewBox="-1600 -1000 3200 2000">
            {Object.values(PATHS).map((d) => (
              <path key={d} d={d} className="hx-dash" fill="none" stroke="#a9bbf0" strokeWidth="2.5" strokeDasharray="7 10" strokeLinecap="round" />
            ))}
          </svg>

          {/* depth: a blurred tile floating far back, a faded one up front */}
          <span className="absolute left-[-180px] top-[-700px] opacity-60 blur-[3px]"><span className="hx-bob block"><TileFace def={{ icon: CreditCard, color: "#3366ff" }} size={58} /></span></span>
          <span className="absolute left-[-760px] top-[330px] opacity-50 blur-[1.5px]"><TileFace def={{ icon: Receipt, color: "#f2735f" }} /></span>
          <span className="absolute left-[-470px] top-[300px]"><TileFace def={{ icon: FileSpreadsheet, color: "#1f9d55" }} /></span>
          <span className="absolute left-[-250px] top-[430px] opacity-80"><TileFace def={{ icon: Landmark, color: "#3366ff" }} /></span>

          {/* incoming export rows */}
          <PathTiles tiles={LOW_TILES} path={PATHS.low} duration={13} rest={[35, 80]} />
          <PathTiles tiles={FRONT_TILES} path={PATHS.front} duration={11} rest={[30, 75]} />
          <PathTiles tiles={MAIN_TILES} path={PATHS.main} duration={21} rest={[8, 22, 36, 50, 64, 78, 92]} />

          {/* outgoing report cards */}
          {CARDS.map((c, i) => <ReportCard key={c.label} index={i} label={c.label}>{c.body}</ReportCard>)}

          <Disc />
        </motion.div>
      </div>
    </div>
  );
}
