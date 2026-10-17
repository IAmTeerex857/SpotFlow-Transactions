/** Breadcrumb, title, description and actions at the top of every page. */
export function PageHeader({ crumb, title, description, actions }: {
  crumb?: string; title: React.ReactNode; description?: React.ReactNode; actions?: React.ReactNode;
}) {
  return (
    <div className="flex flex-wrap items-end justify-between gap-4">
      <div className="min-w-0">
        {crumb && <p className="mb-1.5 text-[12.5px] text-faint">{crumb}</p>}
        <h1 className="font-display text-[24px] font-semibold tracking-[-0.03em] text-ink sm:text-[26px]">{title}</h1>
        {description && <p className="mt-1 text-[13.5px] text-muted">{description}</p>}
      </div>
      {actions && <div className="flex flex-wrap items-center gap-2">{actions}</div>}
    </div>
  );
}
