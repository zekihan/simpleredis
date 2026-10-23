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
