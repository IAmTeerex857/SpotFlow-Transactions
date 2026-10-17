import { ExportIllo, ProvidersIllo, ReasonsIllo, RegionsIllo, RetriesIllo, VolumeIllo } from "@/components/landing/art/iso";
import { Reveal, Spotlight } from "@/components/landing/motion";
import { cn } from "@/lib/utils";
import { Heading, container, ink } from "./heading";

const ITEMS = [
  { title: "Why payments fail", body: "Provider messages grouped into clear reasons, so “insufficient funds” isn't split across twelve spellings.", Illo: ReasonsIllo, tag: "Failures" },
  { title: "Provider health", body: "Which providers convert, which are slipping, and the reason behind each failure.", Illo: ProvidersIllo, tag: "Providers" },
  { title: "Regional performance", body: "Every market side by side: volume, success rate and what breaks where.", Illo: RegionsIllo, tag: "Regions" },
  { title: "Customer retries", body: "Who tried again, how long they waited, and whether they eventually paid.", Illo: RetriesIllo, tag: "Retries" },
  { title: "Merchant volume", body: "Successful and failed volume per merchant, converted to USD at the export's rate.", Illo: VolumeIllo, tag: "Merchants" },
  { title: "Every export kept", body: "Each upload is versioned, so any day can be re-run when the parser learns something new.", Illo: ExportIllo, tag: "History" },
];

export function Product() {
  return (
    <section id="product" className="scroll-mt-16 border-y border-line bg-white py-24 sm:py-32">
      <div className={container}>
        <Heading center eyebrow="Product" title="Everything the morning report needs." body="Six views of the same export, each answering the question someone will ask at stand-up." />
        <div className="mt-16 grid gap-px overflow-hidden rounded-[28px] border border-line bg-line sm:grid-cols-2 lg:grid-cols-3">
          {ITEMS.map((item, i) => (
            <Spotlight key={item.title} className="group bg-white">
              <Reveal delay={(i % 3) * 0.08} className="flex h-full flex-col">
                <div className="relative flex h-[230px] items-center justify-center overflow-hidden bg-gradient-to-b from-[#f5f7fd] to-white">
                  <div aria-hidden className="dot-grid absolute inset-0 opacity-50 [mask-image:linear-gradient(to_bottom,black,transparent)]" />
                  <span className="absolute left-5 top-5 rounded-full border border-line bg-white px-2.5 py-0.5 text-[11.5px] font-medium text-muted">{item.tag}</span>
                  <item.Illo className="relative w-[78%] max-w-[330px] transition-transform duration-500 ease-out group-hover:-translate-y-1.5 group-hover:scale-[1.02]" />
                </div>
                <div className="p-7 pt-5">
                  <h3 className={cn("font-display text-[20px] font-semibold tracking-[-0.03em]", ink)}>{item.title}</h3>
                  <p className="mt-2 text-[14.5px] leading-relaxed text-muted">{item.body}</p>
                </div>
              </Reveal>
            </Spotlight>
          ))}
        </div>
      </div>
    </section>
  );
}
