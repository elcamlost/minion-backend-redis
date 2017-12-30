package Minion::Backend::Redis;
use Mojo::Base 'Minion::Backend';

use Carp 'croak';
use Data::MessagePack;
use Mojo::Util qw(sha1_sum encode);

#@returns Mojo::Redis2
has 'redis' => sub { croak 'Mojo::Redis2 instance is not provided' };

has 'ns' => sub { '_mibare1' };

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
        local id = redis.call("INCR", "last_job_id");
        local key = KEYS[1] .. ":job:" .. id;
        redis.call("HMSET", key, unpack(ARGV));
        local i = 1;
        while i < #ARGV do
            local k = ARGV[i];
            local v = ARGV[i + 1];
            i = i + 2;
            if k == "queue" then
                local queue_key = KEYS[1] .. ":job_queue:" .. v
                redis.call("SADD", queue_key, id);
            elseif k == "state" then
                local state_key = KEYS[1] .. ":job_state:" .. v
                redis.call("SADD", state_key, id);
            elseif k == "task" then
                local task_key = KEYS[1] .. ":job_task:" .. v
                redis.call("SADD", task_key, id);
            end;
        end;
        return id
    LUA

    return _script($script, 1, $self->ns, (
        args => _pack($args),
        attempts => $options->{attempts} // 1,
        delayed => time() + $options->{delay} // 0,
        notes => _pack($options->{notes} || {}),
        parents => _pack($options->{parents} || []),
        priority => $options->{priority} // 0,
        queue => $options->{queue} // 'default',
        state => 'inactive',
        task => $task,

    ));
}

sub fail_job     { croak 'Method "fail_job" not implemented by subclass' }
sub finish_job   { croak 'Method "finish_job" not implemented by subclass' }
sub list_jobs    { croak 'Method "list_jobs" not implemented by subclass' }
sub list_workers { croak 'Method "list_workers" not implemented by subclass' }
sub lock         { croak 'Method "lock" not implemented by subclass' }
sub note         { croak 'Method "note" not implemented by subclass' }

sub receive {
    my ($self, $id) = @_;

    state $script = <<'    LUA';
        local key = KEYS[1] .. ":worker:" .. KEYS[1]
        if redis.call("HEXISTS", key, KEYS[3]) == 1 then
            local payload = redis.call("HGET", key, KEYS[3])
            redis.call("HDEL", key, KEYS[3])
            return cmsgpack.unpack(payload)
        else
            return nil
        end
    LUA
    return _script($script, 3, $self->ns, $id, 'inbox') // [];
}

sub register_worker {
    croak 'Method "register_worker" not implemented by subclass';
}

sub remove_job { croak 'Method "remove_job" not implemented by subclass' }
sub repair     { croak 'Method "repair" not implemented by subclass' }
sub reset      { croak 'Method "reset" not implemented by subclass' }
sub retry_job  { croak 'Method "retry_job" not implemented by subclass' }
sub stats      { croak 'Method "stats" not implemented by subclass' }
sub unlock     { croak 'Method "unlock" not implemented by subclass' }

sub unregister_worker {
    croak 'Method "unregister_worker" not implemented by subclass';
}

sub _try {
    my ($self, $id, $options) = @_;
    $options //= {};

    state $script_multi = <<'    LUA';
        local ns = KEYS[1];
        local worker_id = KEYS[2];
        local ts = table.remove(ARGV, 1);
        local jobs = {};
        local job_cond_keys = {};
        for k,v in pairs(ARGV) do
            if k == "queue" then
                table.insert(job_cond_keys, ns .. ":job_queue:" .. v)
            end;
            if k == "task" then
                table.insert(job_cond_keys, ns .. ":job_task:" .. v)
            end;
        end;
        table.insert(job_cond_keys, ns .. ":job_task:inactive";
        local job_ids = redis.call("SINTER", job_cond_keys);

        local jobs = {};
        for _, id in job_ids do
            job = redis.call(
                "HMGET", ns .. ":job:" .. id, priority, delayed, args, retries, task
            ));
            local push = 0;
            if (job.delayed <= ts) then
                local parents = job.parents;
                if parents ~= nil and parents ~= '' then
                    local parent_keys = {};
                    for _, parent_id in ipairs(msgpack.unpack(parents)) do
                        local parent_state redis.call("HMGET",  ns .. ":job:" .. parent_id, state)
                        if (1
                            and parent_state ~= "inactive"
                            and parent_state ~= "active"
                            and parent_state ~= "failed"
                        ) then
                            table.insert(jobs, job);
                        end;
                    end;
                else
                    table.insert(jobs, job);
                end;
            end;
        end;

        return jobs;

        return id
    LUA

    return $self->_script($script_multi, 2, $self->ns, $id, time(),
        (map { queue => $_ } @{ $options->{queues} || ['default'] }),
        (map { task => $_ } keys %{$self->minion->tasks}),
    );
}

sub _pack { Data::MessagePack->pack($_[0]) }

sub _script {
    my ($self, $script, @args) = @_;

    my $script_sha = sha1_sum(encode 'UTF-8', $script);
    my $val = $self->redis->evalsha($script_sha, @args) ||
        $self->redis->eval($script, @args);

    return $val;
}


1;
