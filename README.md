# simpleredis
Minimal go redis with only `get`, `set` and `delete` operation.  
It supports password authentication with redis.
With **NO** external dependencies.

## Example
```go
import simpleredis "github.com/maxlerebourg/simpleredis"

var redis simpleredis.SimpleRedis

redis.Init("redis:6379", "", "") // redisHost, redisPass, redisDatabase

err := redis.Set("test", []bytes("whatever"), 60),  // Set key "test" with "whatever" for 60 seconds
if err != nil {
  ...
}
val, err := redis.Get("test") // get key test
if err != nil {
  // err could be only redis:unreachable, redis:miss or redis:timeout available in simpleredis.RedisUnreachable
  ...
}
err = redis.Del("test")
if err != nil {
  ...
}
```

## Author
Max Lerebourg @ [Primadviz.com](https://primadviz.com)
Mathieu Hanotaux

## ACL usernames and error handling

Use `InitWithUsername(host, username, password, database)` for named Redis users.
`Init` remains compatible with password-only AUTH; an empty username also selects
that form. This library does not add a key prefix: callers must namespace all
keys and enforce that namespace through server ACLs.

Every operation uses RESP bulk-string arguments and one 1.9-second internal deadline (within a two-second total budget) for
connection, AUTH, SELECT and the command together. GET replies are limited to
1 MiB. Empty values succeed; only a null bulk reply is a miss. SET and DEL return
server and transport failures. A missing key on DEL is successful. Authentication
failure stops processing; server errors are reduced to classes to avoid exposing
credentials or arguments. No reconnect/retry is performed inside an operation.

Run `go test -race ./...` and `go vet ./...`. The consuming Traefik plugin must
also pass interpreted Yaegi and actual Traefik tests with its vendored copy;
native Go tests alone do not establish plugin compatibility.

## Optional bounded connection reuse

Pooling is disabled by default. After `InitWithUsername`, call
`ConfigurePool(PoolOptions{MaxActive: ..., MaxIdle: ..., MaxWaiters: ...,
IdleTimeout: ..., MaxLifetime: ...})` using measured endpoint concurrency and
resource limits. All bounds must be explicit; active must be positive, idle must
be between zero and active, waiters must be nonnegative, and durations positive.
These are per-client limits, multiplied by middleware instances, processes and
reader/writer endpoints. Queue overflow returns `RedisUnreachable`; a queued
operation exhausting the shared 1.9-second budget returns `RedisTimeout`.

Each borrowed socket is exclusive to one operation and its initialized host,
identity and database. AUTH runs on every checkout to preserve immediate password-revocation behavior;
SELECT also runs on every checkout so revoking its command permission retains
the original failure behavior. The pool saves TCP/DNS churn and buffer allocation;
it does not eliminate AUTH or SELECT operations. Deadlines reset
on every checkout. Protocol/server/transport errors discard sockets; GET misses
retain healthy sockets. Operations never reconnect or replay commands. Expired
idle sockets are reaped by one goroutine per pool; active sockets are discarded
on return once their lifetime expires. Idle expiry is checked on checkout and
periodically, so physical cleanup may take up to another reaper interval.

`GetContext` supports cancellation during pool waiting, dialing and I/O. The
unpooled path retains its original bounded operation behavior after checking
initial cancellation. `Close` wakes waiters, closes borrowed and idle sockets,
and joins the reaper. It is idempotent and may race with operations. A retired
pooled client fails subsequent operations. Call `Close` during owner retirement;
finish fenced lease-release cleanup before closing its writer pool.

Init and pool configuration must not race with operations. Reinitialization
closes the previous pool, including old authenticated sockets; a replacement
pool never shares connections with another client. Redis command ACL changes
still apply on each command. Although Redis password removal alone does not terminate authenticated sessions,
the next checkout reauthenticates and discards a socket whose credentials are
revoked. No command is sent after failed authentication. This library does not
issue administrative commands or broaden ACLs.

This is an experimental opt-in candidate until the consuming plugin's pinned
Traefik/Yaegi, fault, credential, comparison and sustained-soak gates pass.

Pooling requires an initialized authentication identity (username or password)
and an explicit database, including `0` for the default database. Unpooled legacy
clients retain their existing anonymous/default-database behavior.

Keep configured clients by pointer; do not copy a `SimpleRedis` value after
pool configuration. Copies share the pool's retirement state. Existing consumers
construct their reader slice before enabling pooling.
