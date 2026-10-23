package simpleredis

import (
	"bufio"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAcceptanceDeadline(t *testing.T) {
	for _, command := range []string{"AUTH", "SELECT", "GET", "SET", "DEL", "EVAL"} {
		t.Run(command, func(t *testing.T) {
			var wg sync.WaitGroup
			slots := make(chan struct{}, 10)
			for i := 0; i < 100; i++ {
				slots <- struct{}{}
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer func() { <-slots }()
					addr := server(t, func(conn net.Conn) {
						reader := bufio.NewReader(conn)
						for {
							args, err := readCommand(reader)
							if err != nil {
								return
							}
							if strings.ToUpper(args[0]) == command {
								_, _ = io.Copy(io.Discard, reader)
								return
							}
							_, _ = io.WriteString(conn, "+OK\r\n")
						}
					})
					client := SimpleRedis{}
					client.InitWithUsername(addr, "fixture", "fixture", "11")
					started := time.Now()
					var err error
					switch command {
					case "EVAL":
						_, err = client.Eval("return 'OK'", []string{"crowdsec:key"})
					case "SET":
						err = client.Set("crowdsec:key", []byte("f"), 60)
					case "DEL":
						err = client.Del("crowdsec:key")
					default:
						_, err = client.Get("crowdsec:key")
					}
					elapsed := time.Since(started)
					if err == nil || elapsed > 2*time.Second {
						t.Errorf("%s duration=%v error=%v exceeds two-second operation budget", command, elapsed, err)
					}
				}()
			}
			wg.Wait()
		})
	}
}
