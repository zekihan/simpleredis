package simpleredis

import (
	"bufio"
	"io"
	"net"
	"strings"
	"testing"
)

func TestExchangeRejectsBufferedReplyBeforeWriting(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("+OK\r\n$6\r\nforged\r\n"))
	if _, _, err := readReply(r); err != nil {
		t.Fatal(err)
	}
	var sent strings.Builder
	value, err := exchange(bufio.NewWriter(&sent), r, '$', "GET", "key")
	if err == nil || len(value) != 0 || sent.Len() != 0 {
		t.Fatalf("pending reply used for a new command: value=%q err=%v sent=%q", value, err, sent.String())
	}
}

func TestSetupExtraRepliesCannotSatisfyCommands(t *testing.T) {
	for _, pooled := range []bool{false, true} {
		for _, stage := range []string{"AUTH", "SELECT"} {
			name := stage + "/unpooled"
			if pooled {
				name = stage + "/pooled"
			}
			t.Run(name, func(t *testing.T) {
				f := newPoolFixture(t, func(c net.Conn, args []string) bool {
					if args[0] != stage {
						return false
					}
					// Batch unsolicited replies with the valid setup response.
					// They must not stand in for SELECT or GET responses.
					reply := "+OK\r\n$6\r\nforged\r\n"
					if stage == "AUTH" {
						reply = "+OK\r\n" + reply
					}
					_, _ = io.WriteString(c, reply)
					return true
				})
				var client *SimpleRedis
				if pooled {
					client = pooledClient(t, f, testPoolOptions())
				} else {
					client = &SimpleRedis{}
					client.InitWithUsername(f.listener.Addr().String(), "fixture", "password", "11")
				}
				value, err := client.Get("key")
				if err == nil {
					t.Fatalf("unsolicited setup response accepted as GET value: %q", value)
				}
				// The rejected connection must be closed without writing the next
				// command. Join handlers before inspecting server command counts.
				f.listener.Close()
				<-f.accepted
				f.handlers.Wait()
				if f.gets.Load() != 0 || (stage == "AUTH" && f.selects.Load() != 0) {
					t.Fatalf("commands sent after extra %s reply: SELECT=%d GET=%d", stage, f.selects.Load(), f.gets.Load())
				}
				if pooled {
					client.pool.mu.Lock()
					retained := len(client.pool.all)
					client.pool.mu.Unlock()
					if retained != 0 {
						t.Fatal("connection with unsolicited replies retained")
					}
				}
			})
		}
	}
}
