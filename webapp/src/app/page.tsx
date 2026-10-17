import type { Metadata } from "next";
import { Footer } from "@/components/landing/footer";
import { Hero } from "@/components/landing/hero/hero";
import { ScrollProgress } from "@/components/landing/motion";
import { Nav } from "@/components/landing/nav";
import { FinalCta } from "@/components/landing/sections/final-cta";
import { HowItWorks } from "@/components/landing/sections/how-it-works";
import { InsideApp } from "@/components/landing/sections/inside-app";
import { Pipeline } from "@/components/landing/sections/pipeline";
import { Product } from "@/components/landing/sections/product";
import "@/components/landing/landing.css";

export const metadata: Metadata = {
  title: { absolute: "Spotflow Insights · Every failed payment, explained" },
  description: "Spotflow's internal payments intelligence: drop in the daily export and see why payments fail, by provider, region and merchant.",
};

export default function LandingPage() {
  return (
    <div data-theme="light" className="min-h-dvh bg-[#f7f8fa] text-ink">
      <ScrollProgress />
      <Nav />
      <main className="overflow-x-clip">
        <Hero />
        <HowItWorks />
        <Product />
        <InsideApp />
        <Pipeline />
        <FinalCta />
      </main>
      <Footer />
    </div>
  );
}
