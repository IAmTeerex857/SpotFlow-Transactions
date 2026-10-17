import type { Metadata, Viewport } from "next";
import { Geist, Geist_Mono, Inter } from "next/font/google";
import Script from "next/script";
import "generative-loaders/styles.css";
import "./globals.css";

const inter = Inter({ variable: "--font-inter", subsets: ["latin"] });
const geist = Geist({ variable: "--font-geist", subsets: ["latin"] });
// Only for inline code in Ask answers.
const mono = Geist_Mono({ variable: "--font-geist-mono", subsets: ["latin"], preload: false });

export const metadata: Metadata = {
  title: { default: "Spotflow Insights", template: "%s · Spotflow Insights" },
  description: "Upload a Spotflow export and see exactly where payments fail, why, and who retried.",
  icons: { icon: "/icon.svg" },
  robots: { index: false, follow: false },
};

export const viewport: Viewport = {
  themeColor: [
    { media: "(prefers-color-scheme: light)", color: "#f5f5f6" },
    { media: "(prefers-color-scheme: dark)", color: "#0a0a0b" },
  ],
};

// Applies the saved app theme and sidebar state before first paint. Light and expanded are the defaults.
const bootScript = `try{var d=document.documentElement;if(localStorage.getItem("sf-theme")==="dark")d.dataset.theme="dark";if(localStorage.getItem("sf-sidebar")==="collapsed")d.dataset.sidebar="collapsed"}catch(e){}`;

export default function RootLayout({ children }: LayoutProps<"/">) {
  return (
    <html lang="en" className={`${inter.variable} ${geist.variable} ${mono.variable} h-full`} suppressHydrationWarning>
      <body className="min-h-full">
        {children}
        <Script id="boot" strategy="beforeInteractive">{bootScript}</Script>
      </body>
    </html>
  );
}
