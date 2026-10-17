// Isometric SVG kit (Chatsheet / Boomtax reference): white and pale-blue
// tiles, navy tops, one blue accent, outlined platforms, dashed guides and
// four-point sparkles. Server-rendered; the motion is CSS (landing.css) and
// starts when the illustration scrolls into view.
import type { CSSProperties } from "react";
import { cn } from "@/lib/utils";
import { InView } from "@/components/landing/motion";

const COS30 = Math.cos(Math.PI / 6);

interface Palette { top: string; left: string; right: string; edge?: string }

const P = {
  white: { top: "#ffffff", left: "#e9eefa", right: "#d6dff3" },
  pale: { top: "#e3eafc", left: "#cbd8f8", right: "#b9caf3" },
  deep: { top: "#c3d2f7", left: "#aabff2", right: "#97b0ec" },
  blue: { top: "#3366ff", left: "#2954db", right: "#2047be", edge: "#1d3fae" },
  navy: { top: "#1c2752", left: "#121c3d", right: "#0b132e", edge: "#0a1028" },
  coral: { top: "#f2735f", left: "#da5f4b", right: "#c3503e" },
  amber: { top: "#f1b24a", left: "#d99a34", right: "#c2862a" },
  green: { top: "#32b67f", left: "#28a06d", right: "#1f8a5c" },
} satisfies Record<string, Palette>;

const OUTLINE = "#8fa7ef";

/** Motion hook-up: a CSS class from landing.css plus a stagger index. */
interface Anim { className?: string; i?: number }
const anim = (a: Anim) => ({ className: a.className, style: a.i === undefined ? undefined : ({ "--i": a.i } as CSSProperties) });

/** World (x, y, z) → screen. x runs down-right, y down-left, z up. */
function pt(x: number, y: number, z: number, s = 20): [number, number] {
  return [(x - y) * COS30 * s, (x + y) * 0.5 * s - z * s];
}

const poly = (points: [number, number][]) => points.map(([a, b]) => `${a.toFixed(1)},${b.toFixed(1)}`).join(" ");

/** A box with its three visible faces. Rounded by stroking each face in its own colour. */
function Box({ x, y, z = 0, w, d, h, c = P.white, s = 20, outline, round = 2.5, ...a }: {
  x: number; y: number; z?: number; w: number; d: number; h: number; c?: Palette; s?: number; outline?: string; round?: number;
} & Anim) {
  const top = [pt(x, y, z + h, s), pt(x + w, y, z + h, s), pt(x + w, y + d, z + h, s), pt(x, y + d, z + h, s)];
  const left = [pt(x, y + d, z, s), pt(x + w, y + d, z, s), pt(x + w, y + d, z + h, s), pt(x, y + d, z + h, s)];
  const right = [pt(x + w, y, z, s), pt(x + w, y + d, z, s), pt(x + w, y + d, z + h, s), pt(x + w, y, z + h, s)];
  // Fill (stroked in its own colour for soft corners), then a crisp hairline edge.
  const edge = outline ?? c.edge ?? "rgba(72, 104, 196, 0.32)";
  const face = (points: [number, number][], fill: string) => (
    <g>
      <polygon points={poly(points)} fill={fill} stroke={fill} strokeWidth={round} strokeLinejoin="round" />
      <polygon points={poly(points)} fill="none" stroke={edge} strokeWidth={outline ? 1.2 : 0.8} strokeLinejoin="round" />
    </g>
  );
  return (
    <g {...anim(a)}>
      {h > 0 && face(left, c.left)}
      {h > 0 && face(right, c.right)}
      {face(top, c.top)}
    </g>
  );
}

/** Thin rows printed on a tile's top face (the "data" on a file). */
function Rows({ x, y, z, w, d, n = 3, color = "#c3d2f7", s = 20 }: {
  x: number; y: number; z: number; w: number; d: number; n?: number; color?: string; s?: number;
}) {
  return (
    <g stroke={color} strokeWidth={s * 0.12} strokeLinecap="round">
      {Array.from({ length: n }, (_, i) => {
        const yy = y + (d * (i + 1)) / (n + 1);
        const [x1, y1] = pt(x + w * 0.18, yy, z, s);
        const [x2, y2] = pt(x + w * 0.82, yy, z, s);
        return <line key={i} x1={x1} y1={y1} x2={x2} y2={y2} />;
      })}
    </g>
  );
}

/** Dashed vertical guides from a floating tile down to its landing spot. */
function Guides({ x, y, w, d, z0, z1, s = 20 }: { x: number; y: number; w: number; d: number; z0: number; z1: number; s?: number }) {
  const corners: [number, number][] = [[x, y + d], [x + w, y + d], [x + w, y]];
  return (
    <g stroke="#7f98e6" strokeWidth={1} strokeDasharray="3 4" fill="none" className="iso-dash">
      {corners.map(([cx, cy], i) => {
        const [a, b] = pt(cx, cy, z0, s);
        const [, b2] = pt(cx, cy, z1, s);
        return <line key={i} x1={a} y1={b} x2={a} y2={b2} />;
      })}
      <polygon points={poly([pt(x, y, z0, s), pt(x + w, y, z0, s), pt(x + w, y + d, z0, s), pt(x, y + d, z0, s)])} />
    </g>
  );
}

function Sparkle({ cx, cy, r = 8, color = "#3366ff", delay = 0 }: { cx: number; cy: number; r?: number; color?: string; delay?: number }) {
  const k = r * 0.28;
  return (
    <path
      className="iso-twinkle"
      style={{ animationDelay: `${delay}s` }}
      d={`M${cx},${cy - r} Q${cx + k},${cy - k} ${cx + r},${cy} Q${cx + k},${cy + k} ${cx},${cy + r} Q${cx - k},${cy + k} ${cx - r},${cy} Q${cx - k},${cy - k} ${cx},${cy - r}Z`}
      fill={color}
    />
  );
}

function DashPath({ d }: { d: string }) {
  return <path d={d} className="iso-dash" fill="none" stroke="#7f98e6" strokeWidth={1.2} strokeDasharray="3 4" strokeLinecap="round" />;
}

/** Wraps an illustration, centring world origin in the given view box. */
function Iso({ width = 360, height = 260, ox = 180, oy = 70, label, children, className }: {
  width?: number; height?: number; ox?: number; oy?: number; label: string; children: React.ReactNode; className?: string;
}) {
  return (
    <InView className={cn("iso", className)}>
      <svg viewBox={`0 0 ${width} ${height}`} className="block w-full overflow-visible" role="img" aria-label={label}>
        <g transform={`translate(${ox} ${oy})`}>{children}</g>
      </svg>
    </InView>
  );
}

// ── Illustrations ──────────────────────────────────────────────────────────

export function ExportIllo({ className }: { className?: string }) {
  return (
    <Iso label="A stack of export files with one lifting off" className={className} oy={86}>
      <Box x={0} y={0} w={7} d={6} h={0.25} c={P.pale} outline={OUTLINE} />
      {[0, 1, 2].map((i) => (
        <Box key={i} x={1.6} y={1.4} z={0.25 + i * 0.42} w={3.4} d={3} h={0.36} c={P.white} className="iso-pop" i={i} />
      ))}
      <g className="iso-pop" style={{ "--i": 2 } as CSSProperties}><Rows x={1.6} y={1.4} z={1.47} w={3.4} d={3} /></g>
      <Guides x={2.2} y={0.6} w={3.4} d={3} z0={3.1} z1={1.6} />
      <g className="iso-float">
        <Box x={2.2} y={0.6} z={3.1} w={3.4} d={3} h={0.36} c={P.white} />
        <Rows x={2.2} y={0.6} z={3.46} w={3.4} d={3} color="#8fa7ef" />
      </g>
      <Box x={5.6} y={4.2} z={0.25} w={1.1} d={1.1} h={0.5} c={P.green} className="iso-pop" i={4} />
      <Sparkle cx={-70} cy={-18} r={9} />
      <Sparkle cx={-52} cy={0} r={5} delay={0.8} />
    </Iso>
  );
}

export function ReasonsIllo({ className }: { className?: string }) {
  const rows = [P.coral, P.amber, P.coral, P.deep];
  return (
    <Iso label="Stacked failure reasons, each with a coloured marker" className={className} oy={70}>
      {rows.map((c, i) => (
        <g key={i} className="iso-slide" style={{ "--i": i } as CSSProperties}>
          <Box x={i * 0.9} y={i * 1.5} z={0} w={6} d={1.15} h={0.35} c={P.white} />
          <g className="iso-pulse" style={{ animationDelay: `${i * 0.4}s` }}>
            <Box x={i * 0.9 + 0.35} y={i * 1.5 + 0.25} z={0.35} w={0.65} d={0.65} h={0.2} c={c} round={1.5} />
          </g>
          <Rows x={i * 0.9 + 1.2} y={i * 1.5} z={0.35} w={4.5} d={1.15} n={1} />
        </g>
      ))}
    </Iso>
  );
}

export function ProvidersIllo({ className }: { className?: string }) {
  // Back-to-front (smaller x + y first) so nearer tiles overlap farther ones.
  const cells: [number, number][] = [[0, 0], [2.2, 0], [0, 2.2], [4.4, 0], [2.2, 2.2], [4.4, 2.2]];
  return (
    <Iso label="A grid of provider tiles with one raised" className={className} oy={74}>
      <Box x={-0.5} y={-0.5} w={7.4} d={5.2} h={0.25} c={P.pale} outline={OUTLINE} />
      {cells.map(([x, y], i) => x === 2.2 && y === 2.2
        ? (
          <g key={i} className="iso-pop" style={{ "--i": i } as CSSProperties}>
            <g className="iso-float">
              <Box x={x} y={y} z={0.25} w={1.7} d={1.7} h={1.6} c={P.white} />
              <Box x={x + 0.1} y={y + 0.1} z={1.85} w={1.5} d={1.5} h={0.3} c={P.navy} />
              <Box x={x + 0.5} y={y + 0.5} z={2.15} w={0.7} d={0.7} h={0.06} c={P.blue} />
            </g>
          </g>
        )
        : <Box key={i} x={x} y={y} z={0.25} w={1.7} d={1.7} h={0.5} c={(x + y) % 4.4 === 0 ? P.deep : P.white} className="iso-pop" i={i} />)}
    </Iso>
  );
}

export function RegionsIllo({ className }: { className?: string }) {
  const heights = [[1, 2.2, 1.4, 0.8], [1.8, 3.4, 1.1, 1.6], [0.7, 1.5, 2.6, 1]];
  return (
    <Iso label="Columns of different heights, like a map of regional volume" className={className} oy={92}>
      <Box x={-0.4} y={-0.4} w={7.6} d={5.8} h={0.25} c={P.pale} outline={OUTLINE} />
      {heights
        .flatMap((row, j) => row.map((h, i) => ({ i, j, h })))
        .sort((a, b) => a.i + a.j - (b.i + b.j) || a.j - b.j)
        .map(({ i, j, h }, k) => (
          <Box key={`${i}-${j}`} x={i * 1.85} y={j * 1.85} z={0.25} w={1.35} d={1.35} h={h} c={(i === 1 && j === 1) || (i === 2 && j === 2) ? P.blue : P.white} className="iso-rise" i={k} />
        ))}
    </Iso>
  );
}

export function RetriesIllo({ className }: { className?: string }) {
  const [a1, b1] = pt(1.2, 1.2, 1, 20);
  const [a3, b3] = pt(7.8, 1.2, 1, 20);
  return (
    <Iso label="Three attempts connected by a dashed loop, the last one succeeds" className={className} oy={86}>
      <Box x={-0.3} y={-0.3} w={9.6} d={3.2} h={0.25} c={P.pale} outline={OUTLINE} />
      {[[0, 0.6, P.coral], [3.3, 0.6, P.amber], [6.6, 1.1, P.green]].map(([x, h, c], i) => (
        <g key={i} className="iso-pop" style={{ "--i": i * 2 } as CSSProperties}>
          <Box x={x as number} y={0.3} z={0.25} w={2.2} d={2.2} h={h as number} c={P.white} />
          <Box x={(x as number) + 0.45} y={0.75} z={0.25 + (h as number)} w={1.3} d={1.3} h={0.06} c={c as Palette} />
        </g>
      ))}
      <DashPath d={`M${a1},${b1 - 20} C${a1 + 30},${b1 - 70} ${a3 - 30},${b3 - 80} ${a3},${b3 - 30}`} />
      <Sparkle cx={a3 + 26} cy={b3 - 52} r={8} color="#32b67f" />
    </Iso>
  );
}

export function VolumeIllo({ className }: { className?: string }) {
  const stacks = [3, 5, 8];
  let k = 0;
  return (
    <Iso label="Three stacks of plates rising in height" className={className} oy={110}>
      <Box x={-0.4} y={-0.4} w={8} d={3.4} h={0.25} c={P.pale} outline={OUTLINE} />
      {stacks.map((n, j) => (
        <g key={j}>
          {Array.from({ length: n }, (_, i) => (
            <Box key={i} x={j * 2.6} y={0.2} z={0.25 + i * 0.32} w={2} d={2} h={0.26} c={P.white} round={2} className="iso-pop" i={k++ * 0.5} />
          ))}
          <Box x={j * 2.6 + 0.55} y={0.75} z={0.25 + n * 0.32 - 0.06} w={0.9} d={0.9} h={0.06} c={j === 2 ? P.blue : P.deep} round={1} className="iso-pop" i={k * 0.5} />
        </g>
      ))}
    </Iso>
  );
}
