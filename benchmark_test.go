package simpleredis

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkAuthenticatedGet measures client overhead against a loopback RESP
// fixture. It does not model Redis CPU, DNS, network latency or Traefik/Yaegi.
// Run with -cpu=1,4,16 -bench=BenchmarkAuthenticatedGet -benchtime=1000x -count=3.
func BenchmarkAuthenticatedGet(b *testing.B) {
	for _, instances := range []int{1, 4} {
		b.Run(fmt.Sprintf("instances=%d", instances), func(b *testing.B) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				b.Fatal(err)
			}
			var connections, auths, selects, gets atomic.Int64
			var handlers sync.WaitGroup
			accepted := make(chan struct{})
			go func() {
				defer close(accepted)
				for {
					conn, err := listener.Accept()
					if err != nil {
						return
					}
					connections.Add(1)
					handlers.Add(1)
					go func() {
						defer handlers.Done()
						defer conn.Close()
						reader := bufio.NewReader(conn)
						for {
							args, err := readCommand(reader)
							if err != nil {
								return
							}
							reply := "+OK\r\n"
							switch args[0] {
							case "AUTH":
								auths.Add(1)
							case "SELECT":
								selects.Add(1)
							case "GET":
								gets.Add(1)
								reply = "$1\r\nf\r\n"
							default:
								reply = "-ERR unsupported\r\n"
							}
							if _, err := io.WriteString(conn, reply); err != nil {
								return
							}
						}
					}()
				}
			}()
			defer func() { listener.Close(); <-accepted; handlers.Wait() }()
			clients := make([]SimpleRedis, instances)
			for i := range clients {
				clients[i].InitWithUsername(listener.Addr().String(), "fixture", "fixture", "11")
				if size, _ := strconv.Atoi(os.Getenv("SIMPLEREDIS_POOL_SIZE")); size > 0 {
					if err := clients[i].ConfigurePool(PoolOptions{MaxActive: size, MaxIdle: size, MaxWaiters: 64, IdleTimeout: time.Second, MaxLifetime: time.Minute}); err != nil {
						b.Fatal(err)
					}
				}
			}
			defer func() {
				for i := range clients {
					clients[i].Close()
				}
			}()
			var workers atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				client := &clients[int(workers.Add(1)-1)%instances]
				for pb.Next() {
					value, err := client.Get("crowdsec:benchmark")
					if err != nil && os.Getenv("SIMPLEREDIS_DIAGNOSE_DIAL") == "1" {
						conn, dialErr := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
						if conn != nil {
							conn.Close()
						}
						stats, _ := os.ReadFile("/proc/net/sockstat")
						b.Logf("diagnostic dial: %v; sockets: %s", dialErr, stats)
						if dialErr == nil {
							b.Log("diagnostic dial succeeded after client failure")
						}
					}
					if err != nil || string(value) != "f" {
						b.Errorf("GET value=%q error=%v", value, err)
						return
					}
				}
			})
			b.StopTimer()
			for name, count := range map[string]int64{"connections/op": connections.Load(), "AUTH/op": auths.Load(), "SELECT/op": selects.Load(), "GET/op": gets.Load()} {
				b.ReportMetric(float64(count)/float64(b.N), name)
			}
		})
	}
}
