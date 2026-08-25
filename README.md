# simpleredis
Minimal go redis with only `get`, `set` and `delete` operation.  
It supports password authentication with redis.
With **NO** external dependencies.

Connections are pooled: a command reuses an already authenticated connection when
one is idle, so `AUTH` and `SELECT` are paid once per connection instead of once
per command, and no TCP handshake is needed on the hot path. A `SimpleRedis` is
safe for concurrent use and must not be copied once initialized.

## Example
```go
import simpleredis "github.com/maxlerebourg/simpleredis"

var redis simpleredis.SimpleRedis

redis.Init("redis:6379", "", "") // redisHost, redisPass, redisDatabase

err := redis.Set("test", []byte("whatever"), 60) // Set key "test" with "whatever" for 60 seconds
if err != nil {
  ...
}
val, err := redis.Get("test") // get key test
if err != nil {
  // err could be only redis:unreachable, redis:miss, redis:timeout or redis:noauth,
  // available in simpleredis.RedisUnreachable and friends, or the error redis replied
  ...
}
err = redis.Del("test")
if err != nil {
  ...
}
```

## Pool settings
`Init` uses the defaults below. `InitWithOptions` overrides any of them, a zero
field keeping its default.

```go
redis.InitWithOptions("redis:6379", "", "", simpleredis.Options{
  MaxIdleConns: 8,                      // connections kept open between commands
  IdleTimeout:  30 * time.Second,       // drop a connection left unused for that long
  DialTimeout:  2 * time.Second,        // cap on opening a connection
  IOTimeout:    1 * time.Second,        // cap on one command and its reply
})
```

`MaxIdleConns` never blocks a command: past that many idle connections the extra
ones are closed on release rather than pooled. `Close` drains the pool, and the
client stays usable afterwards.

## Author
Max Lerebourg @ [Primadviz.com](https://primadviz.com)
Mathieu Hanotaux
