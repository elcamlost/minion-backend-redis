package Minion::Backend::Redis;
use Mojo::Base 'Minion::Backend';

use Carp 'croak';
use Data::MessagePack;
use Sys::Hostname 'hostname';
use Mojo::Util qw(sha1_sum encode);
use Mojo::Redis2;
use utf8;

#@returns Mojo::Redis2
has 'redis';

#@returns Minion
has 'minion';

has 'ns' => sub { '_mibare1' };

our $VERSION = '0.001';

#has 'redis';

sub new {
    my $self = shift->SUPER::new(redis => Mojo::Redis2->new(url => shift, encoding => undef));

#    my $redis_version = $self->redis->backend->info('server')->{redis_version};
#    croak 'Redis Server 2.8.0 or later is required'
#        if versioncmp($redis_version, '2.8.0') == -1;

    return $self;
}


=encoding utf8

=head1 DATABASE STRUCTURE

We have several redis data structures to emulate minion_jobs, minion_workers and minion_locks

First of all, we're using hashes to emulate jobs and workers tables

For workers, we have $ns:worker:$id as key and other columns as fields.
For jobs, we have $ns:job:$id as key and other columns as fields.

Also, we're using sets to efficiently emulate conditional queries.

We're use messagepack to store json values in redis.

And we extensively use Lua Scripting to perform all tasks within one call to redis.

hmset _mibare1:worker:$wid inbox json_encode([$command, $args]), {})
set _mibare1:job:$id => [$command, $args];

=cut
sub broadcast {
    my ($self, $command, $args, $ids) = @_;
    $args //= [];
    $ids //=[];

    # TODO benchmark this against lua
    my $workers = $self->redis->keys($self->ns . ":workers:*");
    $self->redis->hmset(
        map {
            $_, 'inbox' => _pack([$command, @$args])
        } @$workers
    );
}

=pod

dequeue

my $job_info = $backend->dequeue($worker_id, 0.5);
my $job_info = $backend->dequeue($worker_id, 0.5, {queues => ['important']});

Wait a given amount of time in seconds for a job, dequeue it and transition from inactive to active state, or return undef if queues were empty. Meant to be overloaded in a subclass.

These options are currently available:

id

    id => '10023'

    Dequeue a specific job.
queues

    queues => ['important']

    One or more queues to dequeue jobs from, defaults to default.

These fields are currently available:

args

    args => ['foo', 'bar']

    Job arguments.
id

    id => '10023'

    Job ID.
retries

    retries => 3

    Number of times job has been retried.
task

    task => 'foo'

    Task name.


=cut
sub dequeue {
    my ($self, $id, $wait, $options) = @_;

    if ((my $job = $self->_try($id, $options))) { return $job }
    return undef if Mojo::IOLoop->is_running;

    # pubsub
    $self->redis->subscribe(['minion:job'] => sub { Mojo::IOLoop->stop });
    my $timer = Mojo::IOLoop->timer($wait => sub { Mojo::IOLoop->stop });
    Mojo::IOLoop->start;
    $self->redis->unsubscribe(['minion:job'] => sub { Mojo::IOLoop->remove($timer) });

    return $self->_try($id, $options);
}

sub enqueue {
    my ($self, $task, $args, $options) = (@_);
    $args //= [];
    $options //= {};

    state $script = <<'    LUA';
        local ns = KEYS[1];
        local id = redis.call("INCR", ns .. ":counter:last_job_id");
        local key = ns .. ":job:" .. id;
        redis.call("HMSET", key, "id", id, unpack(ARGV));

        local priority = redis.call("HGET", key, "priority");

        for i = 1, #ARGV, 2 do
            local k = ARGV[i];
            local v = ARGV[i + 1];
            if k == "queue" then
                local queue_key = ns .. ":job_queue:" .. v
                redis.call("ZADD", queue_key, priority, id);
            elseif k == "state" then
                local state_key = ns .. ":job_state:" .. v
                redis.call("ZADD", state_key, priority, id);
            elseif k == "task" then
                local task_key = ns .. ":job_task:" .. v
                redis.call("ZADD", task_key, priority, id);
            elseif k == "parents" then
                local parents = cmsgpack.unpack(v);
                for _, parent_id in ipairs(parents) do
                    local children_key = ns .. ":job_children:" .. parent_id
                    redis.call("SADD", children_key, id);
                end;
            end;
        end;
        return id
    LUA
    return $self->_script($script, 1, $self->ns, _job_to_redis(
        args => $args,
        attempts => $options->{attempts},
        delayed => $options->{delay},
        notes => $options->{notes},
        parents => $options->{parents},
        priority => $options->{priority},
        queue => $options->{queue},
        task => $task,
    ));
}

sub fail_job { shift->_update(1, @_) }

sub finish_job { shift->_update(0, @_) }

sub list_jobs {
    my ($self, $offset, $limit, $options) = @_;
    $options //= {};
    state $script = <<'    LUA';
        local ns = KEYS[1];
        local ids = {};

        local task_keys = {};
        local state_keys = {};
        local queue_keys = {};
        local start = ARGV[1];
        local stop = ARGV[2];
        for i = 3, #ARGV, 2 do
            local k = ARGV[i];
            local v = ARGV[i + 1];
            if k == "id" then
                ids[#ids + 1] = v;
            elseif k == "queue" then
                queue_keys[#queue_keys + 1] =  ns .. ":job_queue:" .. v;
            elseif k == "state" then
                state_keys[#state_keys + 1] =  ns .. ":job_state:" .. v;
            elseif k == "task" then
                task_keys[#task_keys + 1] =  ns .. ":job_task:" .. v;
            end;
        end;

        if #queue_keys >= 1 then
            local queue_ids_key = ns .. ":tmp:jobs_by_queue";
            redis.call("ZUNIONSTORE", queue_ids_key, #queue_keys, unpack(queue_keys));
            table.insert(job_cond_keys, queue_ids_key);
        end;
        if #state_keys >= 1 then
            local state_ids_key = ns .. ":tmp:jobs_by_state";
            redis.call("ZUNIONSTORE", queue_ids_key, #state_keys, unpack(state_keys));
        end;
        if #task_keys >= 1 then
            local task_ids_key = ns .. ":tmp:jobs_by_task";
            redis.call("ZUNIONSTORE", task_ids_key, #task_keys, unpack(task_keys));
        end;

        local job_cond_keys = {};
        if #ids > 0 then
            local ids_key = ns .. ":tmp:list_jobs_ids";
            redis.call("DEL", ids_key);
            redis.call("ZADD", ids_key, 0, unpack(ids));
            job_cond_keys[#job_cond_keys + 1] = ids_key;
        end;

        redis.call("ZINTERSTORE", ns .. ":tmp:listed_jobs", #job_cond_keys, unpack(job_cond_keys), "AGGREGATE", "MAX");
        local job_ids = redis.call("ZREVRANGE",  ns .. ":tmp:listed_jobs", start, stop);
        local jobs = {};
        local children = {};
        for _, id in ipairs(job_ids) do
            local job_key = ns .. ":job:" .. id;
            local job_raw = redis.call("HGETALL", job_key);
            local job = {};
            for i = 1, #job_raw, 2 do
                job[job_raw[i]] = job_raw[i + 1];
            end;
            job["children"] = redis.call("SMEMBERS", ns .. ":job_children:" .. id);
            jobs[#jobs + 1] = cmsgpack.pack(job);
        end;
        jobs[#jobs + 1] = redis.call("ZCARD", ns .. ":tmp:listed_jobs");
        return jobs;
    LUA

    my $jobs = $self->_script($script, 1, $self->ns,
        $offset, $limit + $offset,
        (map { (queue => $_) } @{ $options->{queues} || [] }),
        (map { (task => $_) } @{ $options->{task} || [] }),
        (map { (id => $_) } @{ $options->{ids} || [] }),
        (map { (state => $_) } @{ $options->{state} || [] }),
    );

    my $total = pop @$jobs // 0;
    $jobs = [ map { _job_from_redis($_) } @$jobs];
    return return {jobs => $jobs, total => $total};
}

sub list_workers {
    my ($self, $offset, $limit, $options) = @_;
    state $script = <<'    LUA';
        local ns = KEYS[1];
        local ids = {};
        for i = 1, #ARGV, 2 do
            local k = ARGV[i];
            local v = ARGV[i + 1];
            if k == "id" then
                ids[#ids + 1] = id;
        end;

        local worker_key = {};
        if #ids > 0 then
            worker_key[#worker_ids + 1] = ns .. ":worker:" .. id;
        else
            worker_key = redis.call("KEYS", ns . ":worker:*");
        end;

        local workers = {};
        for _, worker_key in ipairs(worker_keys) do
            local worker_raw = redis.call("HGETALL", worker_key);
            local worker = {};
            for i = 1, #worker_raw, 2 do
                worker[worker_raw[i]] = worker_raw[i + 1];
            end;
            worker_raw["jobs"] = redis.call(ns .. ":active_jobs:" .. worker["id"]);
            workers[#workers + 1] = cmsgpack.pack(workers);
        end;
        return workers;
    LUA

    my $workers = $self->_script($script, 1, $self->ns,
        (map { (id => $_) } @{ $options->{ids} || [] }),
    );
    my $total = scalar(@$workers);
    $workers = [ map { _unpack($_) } @$workers[$offset .. $limit + $offset] ];
    return return {workers => $workers, total => $total};

}

sub lock { croak "not implemented. see no reason" }

# FIXME transaction
sub note {
    my ($self, $id, $key, $value) = @_;
    my $note = $self->redis->hget($self->ns . ':job:' . $id, 'note');
    return unless $note;

    $note = _unpack($note);
    $note->{$key} = $value;
    $self->redis->hset($self->ns . ':job:' . $id, 'note', _pack($note));
    return 1;
}

sub receive {
    my ($self, $id) = @_;

    state $script = <<'    LUA';
        local key = KEYS[1] .. ":worker:" .. KEYS[1]
        if redis.call("HEXISTS", key, KEYS[3]) == 1 then
            local payload = redis.call("HGET", key, "inbox")
            redis.call("HDEL", key, "inbox")
            return payload
        else
            return nil
        end
    LUA
    my $inbox = $self->_script($script, 2, $self->ns, $id);
    return $inbox
        ?  _unpack($inbox)
        : [];
}

sub register_worker {
    my ($self, $id, $options) = @_;
    $options //= {};

    state $script = <<'    LUA';
        local ns = KEYS[1];
        local id = KEYS[2];
        if id == nil or id == "" then
            id = redis.call("INCR", ns .. ":counter:last_worker_id");
        end;
        local key = ns .. ":worker:" .. id;
        redis.call("HMSET", key, unpack(ARGV));

        return id;
    LUA

    return $self->_script($script, 2, $self->ns, $id // '',
        host => $self->{host} //= hostname(),
        pid => $$,
        status => _pack($options->{status} // {}),
        started => time(),
    );

}

sub remove_job {
    my ($self, $id) = @_;
    state $script = <<'    LUA';
        local ns = KEYS[1];
        local remove_after = KEYS[3];
        local id = KEYS[2];
        local key = ns .. ":job:" .. id;
        local job = redis.call("HMGET", key, "state", "queue", "task", "parents");
        local state = job["state"];

        if state == nil or state == "" then
            return nil;
        end;

        if state == "inactive" or state == "failed" or state == "finished" then
            redis.call("SREM", ns .. ":job_state:" .. job[1], id);
            redis.call("SREM", ns .. ":job_queue:" .. job[2], id);
            redis.call("SREM", ns .. ":job_task:" .. job[3], id);
            local parents = cmsgpack.unpack(job.parents);
            if #parents >=1 then
                for _, parent_id in ipairs(parents) do
                    redis.call("SREM", ns .. ":job_children:" .. parent_id, id);
                    local parent_key = ns .. ":job:" .. parent_id;
                    local parent_state = redis.call("HMGET", parent_key, "state");
                    local parent_ttl = redis.call("TTL", parent_key);
                    if parent_ttl < 0 and parent_state == "finished" then
                        redis.call("EXPIRE", parent_key, remove_after);
                    end;
                end;
            end;
            redis.call("DEL", key);
            return true;
        end;
    LUA

    return !!$self->_script($script, 3, $self->ns, $id, $self->minion->remove_after);
}

# about automatic deletion
# we can delete jobs which are satisfied following criteria
# 1) job is finished
# 2) job was finished remove_after seconds ago
# 3) job has no unfinished children
#
# So we can use redis expire feature by this algorithm
# there is no expire for enqueue stage
# we set expire during finish stage for childless jobs
# and 1) we delete finished job from parent's children
#     2) we set expire to parent job in case parent is finished and there is no more children left
sub repair {}
sub reset {
    my ($self) = @_;
    my $cursor = $self->redis->scan(0, MATCH => $self->ns . '*', COUNT => 100);
    my $keys = $cursor->all(COUNT => 100);
    foreach my $key (@$keys) {
        $self->redis->del($key);
    }
}

sub retry_job {
    my ($self, $id, $retries, $options) = @_;
    $options //= {};

    state $script = <<'    LUA';
        local ns = KEYS[1];
        local id = KEYS[2];
        local retries = KEYS[3];
        local key = ns .. ":job:" .. key;

        local job_raw = redis.call("HGETALL", key);
        local job = {};
        for i = 1, #job_raw, 2 do
            job[job_raw[i]] = job_raw[i + 1];
        end;
        if #job == 0 or job["retries"] != retries then
            return nil;
        end;

        local hmset_argv = {};
        for i = 1, #ARGV, 2 do
            local k = ARGV[i];
            local v = ARGV[i + 1];
            if k == "priority" then
                if v == nil then
                    v = job["priority"];
                end;
            end;

            if k == "attempts" then
                if v == nil then
                    v = job["attempts"];
                end;
            end;

            if k == "queue" then
                if v == nil then
                    v = job["queue"];
                else
                    redis.call("ZREM", ns .. ":job_queue:" .. job["queue"], id);
                    redis.call("ZADD", ns .. ":job_queue:" .. v, job["priority"], id);
                end;
            end;

            if k == "state" then
                redis.call("ZREM", ns .. ":job_state:" .. job["state"], id);
                redis.call("ZADD", ns .. ":job_state:" .. v, job["priority"], id);
            end;

            hmset_argv[i] = k;
            hmset_argv[i + 1] = v;
        end;

        redis.call("HMSET", key, unpack(hmset_argv));
        return true;

    LUA


    return !!$self->_script($script, 3, $self->ns, $id, $retries,
        delayed => time() + ($options->{delay} // 0),
        attempts => $options->{attempts},
        priority => $options->{priority},
        queue => $options->{queue},
        state => 'inactive',
        retries => $retries + 1,
    );
}

sub stats {
    my ($self) = @_;

    # FIXME delayed jobs
    my $delayed_jobs = 0;

    # FIXME active workers
    my $active_workers = 0;

    my $stats = {
        inactive_jobs => $self->redis->zcard($self->ns . ':job_state:inactive'),
        active_jobs => $self->redis->zcard($self->ns . ':job_state:active'),
        failed_jobs => $self->redis->zcard($self->ns . ':job_state:failed'),
        finished_jobs => $self->redis->zcard($self->ns . ':job_state:finished'),
        delayed_jobs => $delayed_jobs,
        active_workers => $active_workers,
        enqueued_jobs => $self->redis->get($self->ns . ':counter:last_job_id'),
        inactive_workers => scalar @{ $self->redis->keys($self->ns . ':worker:*') } - $active_workers,
        uptime => $self->redis->get($self->ns . ':uptime'),
    };
    return $stats;
}
sub unlock {}

sub unregister_worker {
    my ($self, $id) = @_;
    $self->redis->del($self->ns . ':worker:' . $id);
}

sub _try {
    my ($self, $id, $options) = @_;
    $options //= {};

    state $script = <<'    LUA';
        local ns = KEYS[1];
        local worker_id = KEYS[2];
        local ts = ARGV[1];
        local job_id = ARGV[2];

        local job_cond_queue_keys = {};
        local job_cond_task_keys = {};
        local job_cond_keys = {};
        if (job_id ~= nil and job_id ~= "" and job_id ~= 0) then
            local ids_key = ns .. ":tmp:list_jobs_ids";
            redis.call("DEL", ids_key);
            redis.call("ZADD", ids_key, 0, job_id);
            table.insert(job_cond_keys, ids_key);
        end;
        for i = 3, #ARGV, 2 do
            local k = ARGV[i];
            local v = ARGV[i + 1];
            if k == "queue" then
                table.insert(job_cond_queue_keys, ns .. ":job_queue:" .. v);
            end;
            if k == "task" then
                table.insert(job_cond_task_keys, ns .. ":job_task:" .. v);
            end;
        end;
        table.insert(job_cond_keys, ns .. ":job_state:inactive");
        if #job_cond_queue_keys >= 1 then
            local queue_ids_key = ns .. ":tmp:jobs_by_queue";
            redis.call("ZUNIONSTORE", queue_ids_key, #job_cond_queue_keys, unpack(job_cond_queue_keys));
            table.insert(job_cond_keys, queue_ids_key);
        end;
        if #job_cond_task_keys >= 1 then
            local task_ids_key = ns .. ":tmp:jobs_by_task";
            redis.call("ZUNIONSTORE", task_ids_key, #job_cond_task_keys, unpack(job_cond_task_keys));
            table.insert(job_cond_keys, task_ids_key);
        end;

        local cnt = #job_cond_keys;
        table.insert(job_cond_keys, "AGGREGATE");
        table.insert(job_cond_keys, "MAX");
        redis.call("ZINTERSTORE", ns .. ":tmp:dequeued_jobs", cnt, unpack(job_cond_keys));
        local job_ids = redis.call("ZREVRANGE",  ns .. ":tmp:dequeued_jobs", 0, -1);
        for _, id in ipairs(job_ids) do
            local job_key = ns .. ":job:" .. id;
            local job_raw = redis.call("HGETALL", job_key);
            local job = {};
            for i = 1, #job_raw, 2 do
                job[job_raw[i]] = job_raw[i + 1];
            end;

            local push = 0;
            if (job["delayed"] <= ts) then
                local parents = cmsgpack.unpack(job.parents);
                if #parents >=1 then
                    for _, parent_id in ipairs(parents) do
                        local parent_state = redis.call("HGET",  ns .. ":job:" .. parent_id, "state");
                        if (1
                            and parent_state ~= "inactive"
                            and parent_state ~= "active"
                            and parent_state ~= "failed"
                        ) then
                            push = 1;
                        end;
                    end;
                else
                    push = 1;
                end;
            end;

            if push == 1 then
                redis.call("HMSET", job_key, "worker_id", worker_id, "started", ts, "state", "active");
                redis.call("ZREM", ns .. ":job_state:" .. job["state"], id);
                redis.call("ZADD", ns .. ":job_state:active", job["priority"], id);
                redis.call("SADD", ns .. ":active_jobs:" .. worker_id, id);
                job["worker_id"] = worker_id;
                job["started"] = ts;
                job["state"] = "active";
                return cmsgpack.pack(job);
            end;
        end;
    LUA
    my $job = $self->_script($script, 2, $self->ns, $id // 0, time(), $options->{id} // '',
        (map { (queue => $_) } @{ $options->{queues} || ['default'] }),
        (map { (task => $_) } keys %{$self->minion->tasks}),
    );
    return defined $job ? _job_from_redis($job) : undef;
}

sub _update {
    my ($self, $fail, $id, $retries, $result) = @_;

    state $script = <<'    LUA';
        local ns = KEYS[1];
        local id = KEYS[2];
        local k_retries = KEYS[3];
        local remove_after = KEYS[4];

        local is_active = redis.call("ZRANK", ns .. ":job_state:active", id);
        if (is_active == nil) then
            return nil;
        end;

        local key = ns .. ":job:" .. id;
        local retries = redis.call("HGET", key, "retries");
        if (retries ~= k_retries) then
            return nil;
        end;


        local job = {};
        redis.call("HMSET", key, unpack(ARGV));
        local job_raw = redis.call("HGETALL", key);
        for i = 1, #job_raw, 2 do
            job[job_raw[i]] = job_raw[i + 1];
        end;

        if job["state"] == "finished" then
            local children_key = ns .. ":job_children:" .. id;
            local children_cnt = redis.call("SCARD", children_key);
            if (children_cnt == 0) then
                redis.call("EXPIRE", key, remove_after);
            end;
            local parents = cmsgpack.unpack(job["parents"]);
            for _, parent_id in ipairs(parents) do
                local p_children_key = ns .. ":job_children:" .. parent_id;
                redis.call("SREM", p_children_key, id);
                local p_children_cnt = redis.call("SCARD", p_children_key);
                if (p_children_cnt == 0) then
                    redis.call("EXPIRE", ns .. ":job:" .. parent_id, remove_after);
                end;
            end;
        end;

        redis.call("SREM", ns .. ":active_jobs:" .. job["worker_id"], id);
        redis.call("ZREM", ns .. ":job_state:active", id);
        redis.call("ZADD", ns .. ":job_state:" .. job["state"], job["priority"], id);
        return redis.call("HGET", key, "attempts");
    LUA

    return undef unless my $attempts = $self->_script($script,
        4, $self->ns, $id, $retries // 0, $self->minion->remove_after,
        finished => time(),
        result => _pack($result),
        state => $fail ? 'failed' : 'finished',
    );

    return 1 if !$fail || $attempts == 1;
    return 1 if $retries >= ($attempts - 1);
    my $delay = $self->minion->backoff->($retries);
    return $self->retry_job($id, $retries, {delay => $delay});
}

sub _mp { state $mp = Data::MessagePack->new->utf8; }

sub _pack { _mp->pack($_[0]) }

sub _unpack { use Carp;my ($v) = @_; confess "undef" unless defined $v; _mp->unpack($v) }

sub _job_to_redis {
    my %job = @_;
    return (
        args => _pack($job{'args'}),
        attempts => $job{attempts} // 1,
        delayed => time() + ($job{delay} // 0),
        notes => _pack($job{notes} || {}),
        parents => _pack($job{parents} || []),
        priority => $job{priority} // 0,
        queue => $job{queue} // 'default',
        state => 'inactive',
        retries => 0,
        task => $job{task},
    );
}

sub _job_from_redis {
    my ($packed_job) = @_;
    my $job = _unpack($packed_job);
    for my $key (qw/args parents notes result/) {
        next unless defined $job->{$key};
        $job->{$key} = _unpack($job->{$key});
    }
    return $job;
}

sub _script {
    my ($self, $script, @args) = @_;

    state $scripts = {};
    my $sha = sha1_sum(encode 'UTF-8', $script);
#    unless (exists $scripts->{$sha}) {
        $scripts->{$sha} = 1;
        return $self->redis->eval($script, @args);
#    }

#    return $self->redis->evalsha($sha, @args);
}


1;
