-- Spotflow Failed Payments — initial schema.
--
-- Access model: every signed-in @spotflow.one account can read and upload.
-- Sign-ups from any other domain are rejected by a trigger on auth.users, and
-- every table policy re-checks the domain from the JWT, so a session minted
-- for another domain still sees nothing.

-- ── Membership ────────────────────────────────────────────────────────────
create or replace function public.member_domain() returns text
language sql immutable as $$ select 'spotflow.one'::text $$;

create or replace function public.is_member() returns boolean
language sql stable as $$
  select coalesce(lower(split_part(auth.jwt() ->> 'email', '@', 2)) = public.member_domain(), false)
$$;

create or replace function public.enforce_member_domain() returns trigger
language plpgsql security definer set search_path = public as $$
begin
  if new.email is null or lower(split_part(new.email, '@', 2)) <> public.member_domain() then
    raise exception 'Only % accounts can sign in', public.member_domain() using errcode = 'P0001';
  end if;
  return new;
end
$$;

drop trigger if exists enforce_member_domain on auth.users;
create trigger enforce_member_domain
  before insert or update of email on auth.users
  for each row execute function public.enforce_member_domain();

-- ── Registry additions (providers/regions added from the app) ─────────────
create table public.registry_entries (
  kind text not null check (kind in ('provider', 'region')),
  name text not null check (length(trim(name)) between 2 and 64),
  created_by uuid default auth.uid() references auth.users on delete set null,
  created_at timestamptz not null default now(),
  primary key (kind, name)
);

-- ── Uploads ───────────────────────────────────────────────────────────────
create table public.uploads (
  id uuid primary key default gen_random_uuid(),
  uploaded_by uuid default auth.uid() references auth.users on delete set null,
  uploader_email text default (auth.jwt() ->> 'email'),
  source text not null default 'upload' check (source in ('upload', 'legacy_import', 'reprocess')),
  status text not null default 'processing' check (status in ('processing', 'complete', 'failed')),
  file_name text not null,
  file_sha256 text not null,
  file_bytes integer not null default 0,
  parser_version text not null,
  business_date date,
  period_start timestamptz,
  period_end timestamptz,
  source_rows integer not null default 0,
  parsed_count integer not null default 0,
  recovered_fragments integer not null default 0,
  rows_without_transaction integer not null default 0,
  missing_dates integer not null default 0,
  inserted_count integer not null default 0,
  updated_count integer not null default 0,
  unique_customers integer not null default 0,
  unknown_providers jsonb not null default '{}'::jsonb,
  unknown_regions jsonb not null default '{}'::jsonb,
  columns text[] not null default '{}',
  storage_path text,
  error text,
  duration_ms integer,
  created_at timestamptz not null default now(),
  completed_at timestamptz
);

create index uploads_created_at_idx on public.uploads (created_at desc);
create index uploads_sha_idx on public.uploads (file_sha256);

-- ── Transactions ──────────────────────────────────────────────────────────
-- txn_key is the export's Reference when present, otherwise a hash of the
-- transaction's fields plus its occurrence index within the file. Re-uploading
-- the same export therefore updates rows instead of duplicating them.
create table public.transactions (
  id bigint generated always as identity primary key,
  txn_key text not null unique,
  upload_id uuid not null references public.uploads on delete cascade,
  reference text,
  merchant text not null default '',
  amount numeric not null default 0,
  currency text not null default '',
  rate numeric not null default 0,
  amount_usd numeric not null default 0,
  provider text not null,
  region text not null,
  status text not null,
  channel text not null,
  message text not null,
  paid_at timestamptz,
  -- UTC date of paid_at, or the export's date when the row had no timestamp.
  business_date date not null,
  -- HMAC pseudonym (stable across days), or legacy:<date>:customer N for the
  -- historical exports whose pseudonyms were assigned per day.
  customer_id text,
  retry_count integer,
  fragment boolean not null default false,
  created_at timestamptz not null default now()
);

create index transactions_business_date_idx on public.transactions (business_date);
create index transactions_upload_idx on public.transactions (upload_id);
create index transactions_customer_idx on public.transactions (customer_id, paid_at) where customer_id is not null;
create index transactions_date_merchant_idx on public.transactions (business_date, merchant);

-- ── Row level security ────────────────────────────────────────────────────
alter table public.uploads enable row level security;
alter table public.transactions enable row level security;
alter table public.registry_entries enable row level security;

create policy "members read uploads" on public.uploads
  for select to authenticated using ((select public.is_member()));
create policy "members create uploads" on public.uploads
  for insert to authenticated with check ((select public.is_member()) and uploaded_by = (select auth.uid()));
create policy "members update uploads" on public.uploads
  for update to authenticated using ((select public.is_member())) with check ((select public.is_member()));
create policy "uploaders delete their uploads" on public.uploads
  for delete to authenticated using ((select public.is_member()) and uploaded_by = (select auth.uid()));

create policy "members read transactions" on public.transactions
  for select to authenticated using ((select public.is_member()));
create policy "members insert transactions" on public.transactions
  for insert to authenticated with check ((select public.is_member()));
create policy "members update transactions" on public.transactions
  for update to authenticated using ((select public.is_member())) with check ((select public.is_member()));

create policy "members read registry" on public.registry_entries
  for select to authenticated using ((select public.is_member()));
create policy "members extend registry" on public.registry_entries
  for insert to authenticated with check ((select public.is_member()));

-- ── Ingest ────────────────────────────────────────────────────────────────
create or replace function public.ingest_transactions(p_upload uuid, p_rows jsonb)
returns jsonb
language plpgsql security invoker set search_path = public as $$
declare
  v_inserted integer;
  v_updated integer;
begin
  with incoming as (
    select *
    from jsonb_to_recordset(p_rows) as r(
      txn_key text, reference text, merchant text, amount numeric, currency text, rate numeric,
      amount_usd numeric, provider text, region text, status text, channel text, message text,
      paid_at timestamptz, business_date date, customer_id text, retry_count integer, fragment boolean
    )
  ),
  upserted as (
    insert into public.transactions as t (
      txn_key, upload_id, reference, merchant, amount, currency, rate, amount_usd, provider, region,
      status, channel, message, paid_at, business_date, customer_id, retry_count, fragment
    )
    select
      txn_key, p_upload, reference, coalesce(merchant, ''), coalesce(amount, 0), coalesce(currency, ''),
      coalesce(rate, 0), coalesce(amount_usd, 0), provider, region, status, channel, message, paid_at,
      business_date, nullif(customer_id, ''), retry_count, coalesce(fragment, false)
    from incoming
    on conflict (txn_key) do update set
      upload_id = excluded.upload_id,
      reference = excluded.reference,
      merchant = excluded.merchant,
      amount = excluded.amount,
      currency = excluded.currency,
      rate = excluded.rate,
      amount_usd = excluded.amount_usd,
      provider = excluded.provider,
      region = excluded.region,
      status = excluded.status,
      channel = excluded.channel,
      message = excluded.message,
      paid_at = excluded.paid_at,
      business_date = excluded.business_date,
      customer_id = excluded.customer_id,
      retry_count = excluded.retry_count,
      fragment = excluded.fragment
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted)
  into v_inserted, v_updated
  from upserted;

  return jsonb_build_object('inserted', v_inserted, 'updated', v_updated);
end
$$;

-- ── Reporting ─────────────────────────────────────────────────────────────
-- Returns the cube consumed by src/lib/report/build.ts. Mirrors
-- cubeFromTransactions / retryStats in src/lib/report/cube.ts.
create or replace function public.report_cube(
  p_from date,
  p_to date,
  p_merchants text[] default null,
  p_providers text[] default null,
  p_regions text[] default null,
  p_channels text[] default null,
  p_upload uuid default null
)
returns jsonb
language sql stable security invoker set search_path = public as $$
  with base as (
    select t.id, t.business_date, coalesce(nullif(t.merchant, ''), '<unknown>') as merchant, t.provider,
           t.region, t.status, t.channel, t.currency, t.amount_usd, t.message, t.paid_at, t.customer_id
    from public.transactions t
    where t.business_date between p_from and p_to
      and (p_upload is null or t.upload_id = p_upload)
      and (p_merchants is null or coalesce(nullif(t.merchant, ''), '<unknown>') = any (p_merchants))
      and (p_providers is null or t.provider = any (p_providers))
      and (p_regions is null or t.region = any (p_regions))
      and (p_channels is null or t.channel = any (p_channels))
  ),
  facts as (
    select jsonb_build_object(
      'd', business_date, 'm', merchant, 'p', provider, 'r', region, 's', status, 'c', channel,
      'cur', currency, 'n', count(*), 'usd', coalesce(sum(amount_usd), 0)
    ) as j
    from base
    group by business_date, merchant, provider, region, status, channel, currency
  ),
  messages as (
    select jsonb_build_object('r', region, 'p', provider, 's', status, 'msg', message, 'n', count(*)) as j
    from base
    group by region, provider, status, message
  ),
  peaks as (
    select jsonb_build_object(
      'dow', dow, 'hour', hour, 'n', count(*),
      'ok', count(*) filter (where status = 'successful'),
      'usd', coalesce(sum(amount_usd) filter (where status = 'successful'), 0)
    ) as j
    from (
      select extract(dow from paid_at at time zone 'UTC')::int as dow,
             extract(hour from paid_at at time zone 'UTC')::int as hour,
             status, amount_usd
      from base
      where paid_at is not null
    ) p
    group by dow, hour
  ),
  customers as (
    select lower(customer_id) as customer_id,
           count(*) as attempts,
           bool_or(status = 'successful') as ever_ok,
           (array_agg(status order by paid_at is null, paid_at, id))[count(*)] as last_status
    from base
    where coalesce(customer_id, '') <> ''
    group by lower(customer_id)
    having count(*) > 1
  ),
  gaps as (
    select extract(epoch from b.paid_at - lag(b.paid_at) over (
      partition by lower(b.customer_id) order by b.paid_at, b.id
    ))::float8 as gap
    from base b
    join customers c on c.customer_id = lower(b.customer_id)
    where b.paid_at is not null
  ),
  gap_stats as (
    select count(*) as n,
           coalesce(avg(gap), 0) as avg_gap,
           coalesce((array_agg(gap order by gap))[floor(count(*) / 2)::int + 1], 0) as median_gap
    from gaps
    where gap >= 0
  )
  select jsonb_build_object(
    'facts', coalesce((select jsonb_agg(j) from facts), '[]'::jsonb),
    'messages', coalesce((select jsonb_agg(j) from messages), '[]'::jsonb),
    'peaks', coalesce((select jsonb_agg(j) from peaks), '[]'::jsonb),
    'retry', jsonb_build_object(
      'retriers', (select count(*) from customers),
      'succeededAfter', (select count(*) from customers where ever_ok),
      'unresolved', (select count(*) from customers where last_status <> 'successful'),
      'attempts', coalesce((select jsonb_object_agg(attempts::text, c) from (
        select attempts, count(*) as c from customers group by attempts) a), '{}'::jsonb),
      'finalStatus', coalesce((select jsonb_object_agg(last_status, c) from (
        select last_status, count(*) as c from customers group by last_status) f), '{}'::jsonb),
      'gapCount', (select n from gap_stats),
      'gapAvgSeconds', (select avg_gap from gap_stats),
      'gapMedianSeconds', (select median_gap from gap_stats),
      'legacyShare', coalesce((select avg((customer_id like 'legacy:%')::int)::float8 from customers), 0)
    )
  )
$$;

create or replace function public.filter_options()
returns jsonb
language sql stable security invoker set search_path = public as $$
  select jsonb_build_object(
    'merchants', coalesce((select jsonb_agg(m order by m) from (
      select distinct coalesce(nullif(merchant, ''), '<unknown>') as m from public.transactions) x), '[]'::jsonb),
    'providers', coalesce((select jsonb_agg(provider order by provider) from (
      select distinct provider from public.transactions) x), '[]'::jsonb),
    'regions', coalesce((select jsonb_agg(region order by region) from (
      select distinct region from public.transactions) x), '[]'::jsonb),
    'channels', coalesce((select jsonb_agg(channel order by channel) from (
      select distinct channel from public.transactions) x), '[]'::jsonb),
    'minDate', (select min(business_date) from public.transactions),
    'maxDate', (select max(business_date) from public.transactions)
  )
$$;

-- A small, filtered sample of rows for the Ask assistant. Never returns more
-- than 50 rows, and customer ids are already pseudonymous.
create or replace function public.sample_transactions(
  p_from date,
  p_to date,
  p_merchants text[] default null,
  p_providers text[] default null,
  p_regions text[] default null,
  p_statuses text[] default null,
  p_message text default null,
  p_limit integer default 20
)
returns jsonb
language sql stable security invoker set search_path = public as $$
  select coalesce(jsonb_agg(row_to_json(s)), '[]'::jsonb)
  from (
    select coalesce(nullif(merchant, ''), '<unknown>') as merchant, amount, currency, round(amount_usd, 2) as amount_usd,
           provider, region, status, channel, message, paid_at, customer_id
    from public.transactions
    where business_date between p_from and p_to
      and (p_merchants is null or coalesce(nullif(merchant, ''), '<unknown>') = any (p_merchants))
      and (p_providers is null or provider = any (p_providers))
      and (p_regions is null or region = any (p_regions))
      and (p_statuses is null or status = any (p_statuses))
      and (p_message is null or message ilike '%' || p_message || '%')
    order by paid_at desc nulls last
    limit least(greatest(coalesce(p_limit, 20), 1), 50)
  ) s
$$;

revoke execute on function public.report_cube, public.filter_options, public.sample_transactions,
  public.ingest_transactions from public, anon;
grant execute on function public.report_cube, public.filter_options, public.sample_transactions,
  public.ingest_transactions to authenticated, service_role;

-- ── Storage: scrubbed copies of each upload, for reprocessing ─────────────
-- Emails are replaced with pseudonyms before the copy is written, so the
-- bucket never holds customer contact details.
do $$
begin
  if exists (select 1 from pg_namespace where nspname = 'storage') then
    insert into storage.buckets (id, name, public) values ('exports', 'exports', false)
    on conflict (id) do nothing;

    drop policy if exists "members read exports" on storage.objects;
    create policy "members read exports" on storage.objects
      for select to authenticated using (bucket_id = 'exports' and (select public.is_member()));
    drop policy if exists "members write exports" on storage.objects;
    create policy "members write exports" on storage.objects
      for insert to authenticated with check (bucket_id = 'exports' and (select public.is_member()));
  end if;
end
$$;
