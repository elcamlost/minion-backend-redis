@@ minion
-- 1 up
create type minion_state as enum ('inactive', 'active', 'failed', 'finished');

create table if not exists minion_jobs (
    id       bigserial not null primary key,
    args     jsonb not null
        check(jsonb_typeof(args) = 'array'),
    created  timestamp with time zone not null default now(),
    delayed  timestamp with time zone not null,
    finished timestamp with time zone,
    priority int not null,
    result   jsonb,
    retried  timestamp with time zone,
    retries  int not null default 0,
    started  timestamp with time zone,
    state    minion_state default 'inactive',
    task     text not null,
    worker   bigint,
    queue    text not null default 'default',
    attempts int not null default 1,
    parents  bigint[] not null default '{}',
    notes    jsonb
        check(jsonb_typeof(notes) = 'object') not null default '{}'
);
create table if not exists minion_workers (
    id      bigserial not null primary key,
    host    text not null,
    pid     int not null,
    started timestamp with time zone not null default now(),
    notified timestamp with time zone not null default now(),
    inbox jsonb
        check(jsonb_typeof(inbox) = 'array') not null default '[]',
    status jsonb
        check(jsonb_typeof(status) = 'object') not null default '{}'
);

create or replace function minion_jobs_notify_workers() returns trigger as $$
begin
    if new.delayed <= now() then
        notify "minion.job";
    end if;
    return null;
end;
$$ language plpgsql;
set client_min_messages to warning;
drop trigger if exists minion_jobs_insert_trigger on minion_jobs;
drop trigger if exists minion_jobs_notify_workers_trigger on minion_jobs;
set client_min_messages to notice;
create trigger minion_jobs_notify_workers_trigger
    after insert or update of retries on minion_jobs
    for each row execute procedure minion_jobs_notify_workers();

create index on minion_jobs (state, priority desc, id);

create index on minion_jobs using gin (parents);

create table if not exists minion_locks (
    id      bigserial not null primary key,
    name    text not null,
    expires timestamp with time zone not null
);


alter table minion_locks set unlogged;
create index on minion_locks (name, expires);

create or replace function minion_lock(text, int, int) returns bool as $$
declare
    new_expires timestamp with time zone = now() + (interval '1 second' * $2);
begin
    lock table minion_locks in exclusive mode;
    delete from minion_locks where expires < now();
    if (select count(*) >= $3 from minion_locks where name = $1) then
        return false;
    end if;
    if new_expires > now() then
        insert into minion_locks (name, expires) values ($1, new_expires);
    end if;
    return true;
end;
$$ language plpgsql;