package simpleredis

import (
	"bufio"
	"io"
	"net"
	"strings"
	"testing"
)

func TestLongErrorReplyClasses(t *testing.T) {
	for _, pooled := range []bool{false, true} {
		for _, tc := range []struct{ code, want string }{
			{"ERR", RedisIssue}, {"NOAUTH", RedisNoAuth}, {"WRONGPASS", RedisNoAuth}, {"NOPERM", "redis:noperm"},
		} {
			name := tc.code + "/unpooled"
			if pooled {
				name = tc.code + "/pooled"
			}
			t.Run(name, func(t *testing.T) {
				f := newPoolFixture(t, func(c net.Conn, args []string) bool {
					if args[0] != "GET" {
						return false
					}
					// Redis errors may quote command arguments longer than bufio's default
					// buffer. The detail must not change the public error class or leak out.
					_, _ = io.WriteString(c, "-"+tc.code+" "+strings.Repeat("sensitive-detail", 400)+"\r\n")
					return true
				})
				var client *SimpleRedis
				if pooled {
					client = pooledClient(t, f, testPoolOptions())
				} else {
					client = &SimpleRedis{}
					client.Init(f.listener.Addr().String(), "", "")
				}
				value, err := client.Get("key")
				if len(value) != 0 || err == nil || err.Error() != tc.want {
					t.Fatalf("value=%q err=%v, want %s", value, err, tc.want)
				}
				if pooled {
					client.pool.mu.Lock()
					retained := len(client.pool.all)
					client.pool.mu.Unlock()
					if retained != 0 {
						t.Fatal("error connection retained")
					}
				}
			})
		}
	}
}

func TestReplyLineBoundAndFraming(t *testing.T) {
	for _, reply := range []string{
		"-ERR " + strings.Repeat("x", maxReplySize) + "\r\n",
		"-ERR " + strings.Repeat("x", 5000),
		"-ERR " + strings.Repeat("x", 5000) + "\n",
	} {
		_, _, err := readReply(bufio.NewReader(strings.NewReader(reply)))
		if err == nil || err.Error() == RedisIssue {
			t.Fatalf("invalid error line classified as server error: %v", err)
		}
	}
}
