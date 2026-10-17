// Visual check: screenshots key pages in light and dark at desktop and phone width.
// Usage: npm run screenshots -- [baseUrl] [outDir]   (needs a running server and Google Chrome)
import { mkdirSync } from "node:fs";
import { join } from "node:path";
import { chromium } from "playwright-core";

const base = process.argv[2] ?? "http://localhost:3000";
const out = process.argv[3] ?? ".screenshots";
const chrome = process.env.CHROME_PATH ?? "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
const pages = ["/", "/login", "/demo", "/demo/reports"];
const viewports = [{ name: "desktop", width: 1440, height: 900 }, { name: "phone", width: 390, height: 844 }];

mkdirSync(out, { recursive: true });
const browser = await chromium.launch({ executablePath: chrome });
let failures = 0;
for (const theme of ["light", "dark"]) {
  for (const vp of viewports) {
    const page = await browser.newPage({ viewport: { width: vp.width, height: vp.height } });
    await page.addInitScript((t) => localStorage.setItem("sf-theme", t), theme);
    const errors = [];
    page.on("pageerror", (e) => errors.push(String(e)));
    page.on("console", (m) => m.type() === "error" && errors.push(m.text()));
    for (const path of pages) {
      await page.goto(base + path, { waitUntil: "networkidle" });
      await page.waitForTimeout(1200);
      const file = join(out, `${theme}-${vp.name}${path === "/" ? "-home" : path.replaceAll("/", "-")}.png`);
      await page.screenshot({ path: file });
      const overflow = await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth);
      if (overflow || errors.length) failures++;
      console.log(`${overflow || errors.length ? "✗" : "✓"} ${file}${overflow ? "  horizontal overflow" : ""}${errors.length ? `  ${errors.length} console error(s): ${errors[0].slice(0, 120)}` : ""}`);
      errors.length = 0;
    }
    await page.close();
  }
}
await browser.close();
process.exit(failures ? 1 : 0);
