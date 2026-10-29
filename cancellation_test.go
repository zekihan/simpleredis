package simpleredis

import (
	"context"
	"io"
	"net"
	"testing"
	"time"
)

func TestPoolSaturatedAdmissionPreservesContextError(t *testing.T) {
	for _, expired := range []bool{false, true} {
		name := "canceled"
		if expired {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			entered := make(chan struct{}, 1)
			f := newPoolFixture(t, func(conn net.Conn, args []string) bool {
				if args[0] == "GET" && args[1] == "blocked" {
					entered <- struct{}{}
					_, _ = io.Copy(io.Discard, conn)
					return true
				}
				return false
			})
			o := testPoolOptions()
			o.MaxActive, o.MaxIdle, o.MaxWaiters = 1, 1, 0
			c := pooledClient(t, f, o)
			active, cancelActive := context.WithCancel(context.Background())
			defer cancelActive()
			result := make(chan error, 1)
			go func() { _, err := c.GetContext(active, "blocked"); result <- err }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("first operation did not reach GET")
			}
			if _, err := c.GetContext(context.Background(), "overflow"); err == nil || err.Error() != RedisUnreachable {
				t.Fatalf("live context overflow: %v", err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			want := context.Canceled.Error()
			if expired {
				cancel()
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				want = RedisTimeout
			}
			cancel()
			_, err := c.GetContext(ctx, "must-not-send")
			// Finish the blocked operation before asserting, including on failure.
			cancelActive()
			if activeErr := <-result; activeErr != context.Canceled {
				t.Fatalf("active cancellation: %v", activeErr)
			}
			if err == nil || err.Error() != want {
				t.Fatalf("saturated pool returned %v, want %s", err, want)
			}
			if f.connections.Load() != 1 || f.gets.Load() != 1 {
				t.Fatal("canceled operation contacted the server")
			}
			if len(c.pool.slots) != 0 || len(c.pool.waiters) != 0 {
				t.Fatal("cancellation leaked pool capacity")
			}
			if value, err := c.Get("recovered"); err != nil || string(value) != "f" {
				t.Fatalf("pool did not recover: %q %v", value, err)
			}
		})
	}
}
