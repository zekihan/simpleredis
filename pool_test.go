package simpleredis

import (
	"bufio"
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type poolFixture struct {
	listener    net.Listener
	connections atomic.Int64
	auths       atomic.Int64
	selects     atomic.Int64
	gets        atomic.Int64
	mu          sync.Mutex
	sockets     map[net.Conn]bool
	handlers    sync.WaitGroup
	accepted    chan struct{}
	handle      func(net.Conn, []string) bool
}

func newPoolFixture(t *testing.T, handle func(net.Conn, []string) bool) *poolFixture {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &poolFixture{listener: listener, sockets: make(map[net.Conn]bool), accepted: make(chan struct{}), handle: handle}
	go func() {
		defer close(f.accepted)
		for {
			c, err := listener.Accept()
			if err != nil {
				return
			}
			f.connections.Add(1)
			f.mu.Lock()
			f.sockets[c] = true
			f.mu.Unlock()
			f.handlers.Add(1)
			go func() {
				defer f.handlers.Done()
				defer c.Close()
				defer func() { f.mu.Lock(); delete(f.sockets, c); f.mu.Unlock() }()
				r := bufio.NewReader(c)
				for {
					args, err := readCommand(r)
					if err != nil {
						return
					}
					switch args[0] {
					case "AUTH":
						f.auths.Add(1)
					case "SELECT":
						f.selects.Add(1)
					case "GET":
						f.gets.Add(1)
					}
					if f.handle != nil && f.handle(c, args) {
						continue
					}
					reply := "+OK\r\n"
					if args[0] == "GET" {
						reply = "$1\r\nf\r\n"
					}
					if _, err := io.WriteString(c, reply); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		listener.Close()
		<-f.accepted
		f.mu.Lock()
		for c := range f.sockets {
			c.Close()
		}
		f.mu.Unlock()
		f.handlers.Wait()
	})
	return f
}
func pooledClient(t *testing.T, f *poolFixture, options PoolOptions) *SimpleRedis {
	t.Helper()
	c := &SimpleRedis{}
	c.InitWithUsername(f.listener.Addr().String(), "fixture", "password", "11")
	if err := c.ConfigurePool(options); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}
func testPoolOptions() PoolOptions {
	return PoolOptions{MaxActive: 2, MaxIdle: 2, MaxWaiters: 2, IdleTimeout: time.Second, MaxLifetime: time.Minute}
}
func TestPoolReuseAndMiss(t *testing.T) {
	var miss atomic.Bool
	f := newPoolFixture(t, func(c net.Conn, args []string) bool {
		if args[0] == "GET" && miss.Load() {
			io.WriteString(c, "$-1\r\n")
			return true
		}
		return false
	})
	c := pooledClient(t, f, testPoolOptions())
	for i := 0; i < 20; i++ {
		if _, err := c.Get("key"); err != nil {
			t.Fatal(err)
		}
	}
	miss.Store(true)
	if _, err := c.Get("key"); err == nil || err.Error() != RedisMiss {
		t.Fatal(err)
	}
	miss.Store(false)
	if _, err := c.Get("key"); err != nil {
		t.Fatal(err)
	}
	if f.connections.Load() != 1 || f.auths.Load() != 22 || f.selects.Load() != 22 {
		t.Fatalf("connections=%d auth=%d select=%d", f.connections.Load(), f.auths.Load(), f.selects.Load())
	}
}
func TestPoolBoundedWaitAndCancellation(t *testing.T) {
	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	defer close(release)
	f := newPoolFixture(t, func(c net.Conn, args []string) bool {
		if args[0] == "GET" {
			entered <- struct{}{}
			<-release
		}
		return false
	})
	options := testPoolOptions()
	options.MaxActive = 1
	options.MaxIdle = 1
	options.MaxWaiters = 1
	c := pooledClient(t, f, options)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan error, 1)
	go func() { _, err := c.GetContext(ctx, "key"); first <- err }()
	<-entered
	waiting, cancelWaiting := context.WithCancel(context.Background())
	defer cancelWaiting()
	second := make(chan error, 1)
	go func() { _, err := c.GetContext(waiting, "key"); second <- err }()
	deadline := time.Now().Add(time.Second)
	for len(c.pool.waiters) != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(c.pool.waiters) != 1 {
		t.Fatal("waiter did not queue")
	}
	if _, err := c.Get("key"); err == nil || err.Error() != RedisUnreachable {
		t.Fatalf("queue overflow: %v", err)
	}
	cancelWaiting()
	if err := <-second; err != context.Canceled {
		t.Fatal(err)
	}
	cancel()
	if err := <-first; err != context.Canceled {
		t.Fatal(err)
	}
	if f.connections.Load() != 1 {
		t.Fatal("active bound exceeded")
	}
}
func TestPoolRetirementClosesBorrowedAndWakesWaiters(t *testing.T) {
	entered := make(chan struct{}, 1)
	f := newPoolFixture(t, func(c net.Conn, args []string) bool {
		if args[0] == "GET" {
			entered <- struct{}{}
			io.Copy(io.Discard, c)
			return true
		}
		return false
	})
	options := testPoolOptions()
	options.MaxActive = 1
	options.MaxIdle = 1
	c := pooledClient(t, f, options)
	results := make(chan error, 2)
	go func() { _, err := c.Get("key"); results <- err }()
	<-entered
	go func() { _, err := c.Get("key"); results <- err }()
	c.Close()
	c.Close()
	for i := 0; i < 2; i++ {
		select {
		case err := <-results:
			if err == nil {
				t.Fatal("retired operation succeeded")
			}
		case <-time.After(time.Second):
			t.Fatal("retirement did not unblock")
		}
	}
	if _, err := c.Get("key"); err == nil {
		t.Fatal("retired client reopened")
	}
}
func TestPoolMalformedAndStaleSocketDiscard(t *testing.T) {
	for _, reply := range []string{"?invalid\r\n", "$8\r\nx", "$2\r\nxx!!", "-WRONGPASS secret\r\n", "-NOPERM key\r\n"} {
		t.Run(reply, func(t *testing.T) {
			var broken atomic.Bool
			broken.Store(true)
			f := newPoolFixture(t, func(c net.Conn, args []string) bool {
				if args[0] == "GET" && broken.Swap(false) {
					io.WriteString(c, reply)
					c.Close()
					return true
				}
				return false
			})
			c := pooledClient(t, f, testPoolOptions())
			if _, err := c.Get("key"); err == nil {
				t.Fatal("invalid reply succeeded")
			}
			if _, err := c.Get("key"); err != nil {
				t.Fatal(err)
			}
			if f.connections.Load() != 2 {
				t.Fatal("bad connection reused")
			}
		})
	}
}
func TestPoolIdleAndLifetimeCleanup(t *testing.T) {
	for _, lifetime := range []bool{false, true} {
		t.Run(map[bool]string{false: "idle", true: "lifetime"}[lifetime], func(t *testing.T) {
			f := newPoolFixture(t, nil)
			options := testPoolOptions()
			if lifetime {
				options.MaxLifetime = 20 * time.Millisecond
			} else {
				options.IdleTimeout = 20 * time.Millisecond
			}
			c := pooledClient(t, f, options)
			if _, err := c.Get("key"); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				c.pool.mu.Lock()
				n := len(c.pool.all)
				c.pool.mu.Unlock()
				if n == 0 {
					break
				}
				time.Sleep(time.Millisecond)
			}
			c.pool.mu.Lock()
			n := len(c.pool.all)
			c.pool.mu.Unlock()
			if n != 0 {
				t.Fatal("idle socket retained")
			}
			if _, err := c.Get("key"); err != nil {
				t.Fatal(err)
			}
			if f.connections.Load() != 2 {
				t.Fatal("expired socket reused")
			}
		})
	}
}
func TestPoolDeadlineResetsOnReuse(t *testing.T) {
	var delay atomic.Bool
	f := newPoolFixture(t, func(c net.Conn, args []string) bool {
		if args[0] == "GET" && delay.Load() {
			time.Sleep(60 * time.Millisecond)
		}
		return false
	})
	c := pooledClient(t, f, testPoolOptions())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := c.GetContext(ctx, "key"); err != nil {
		t.Fatal(err)
	}
	delay.Store(true)
	time.Sleep(40 * time.Millisecond)
	if _, err := c.Get("key"); err != nil {
		t.Fatal("old deadline retained", err)
	}
	if f.connections.Load() != 1 {
		t.Fatal("connection not reused")
	}
}
func TestPoolReinitializationIsolatesIdentity(t *testing.T) {
	f := newPoolFixture(t, nil)
	c := pooledClient(t, f, testPoolOptions())
	if _, err := c.Get("key"); err != nil {
		t.Fatal(err)
	}
	old := c.pool
	c.InitWithUsername(f.listener.Addr().String(), "new-user", "new-password", "12")
	if !old.closed {
		t.Fatal("old identity retained")
	}
	if err := c.ConfigurePool(testPoolOptions()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get("key"); err != nil {
		t.Fatal(err)
	}
	if f.auths.Load() != 2 || f.selects.Load() != 2 {
		t.Fatal("new identity did not authenticate/select")
	}
}

func TestPoolConcurrentCheckout(t *testing.T) {
	f := newPoolFixture(t, nil)
	o := testPoolOptions()
	o.MaxActive = 4
	o.MaxIdle = 4
	o.MaxWaiters = 32
	c := pooledClient(t, f, o)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 50; n++ {
				v, err := c.Get("key")
				if err != nil || string(v) != "f" {
					t.Errorf("GET=%q %v", v, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if f.connections.Load() > 4 {
		t.Fatalf("connections=%d exceeds active bound", f.connections.Load())
	}
	c.pool.mu.Lock()
	idle, all := len(c.pool.idle), len(c.pool.all)
	c.pool.mu.Unlock()
	if idle > 4 || all > 4 {
		t.Fatalf("idle=%d all=%d", idle, all)
	}
}
func TestPoolWaitingDeadline(t *testing.T) {
	entered := make(chan struct{}, 1)
	f := newPoolFixture(t, func(c net.Conn, args []string) bool {
		if args[0] == "GET" {
			entered <- struct{}{}
			io.Copy(io.Discard, c)
			return true
		}
		return false
	})
	o := testPoolOptions()
	o.MaxActive = 1
	o.MaxIdle = 1
	c := pooledClient(t, f, o)
	first := make(chan error, 1)
	go func() { _, err := c.Get("key"); first <- err }()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := c.GetContext(ctx, "key")
	if err == nil || err.Error() != RedisTimeout || time.Since(started) > time.Second {
		t.Fatalf("wait error=%v elapsed=%v", err, time.Since(started))
	}
	c.Close()
	<-first
}
func TestPoolClosedSocketIsDiscardedWithoutRetry(t *testing.T) {
	var closeNext atomic.Bool
	f := newPoolFixture(t, func(c net.Conn, args []string) bool {
		if args[0] == "GET" && closeNext.Swap(false) {
			c.Close()
			return true
		}
		return false
	})
	c := pooledClient(t, f, testPoolOptions())
	if _, err := c.Get("key"); err != nil {
		t.Fatal(err)
	}
	closeNext.Store(true)
	if _, err := c.Get("key"); err == nil {
		t.Fatal("closed socket succeeded")
	}
	if f.connections.Load() != 1 {
		t.Fatal("operation retried on fresh socket")
	}
	if _, err := c.Get("key"); err != nil {
		t.Fatal(err)
	}
	if f.connections.Load() != 2 {
		t.Fatal("stale socket retained")
	}
}
func TestPoolDiscardsBufferedExtraReply(t *testing.T) {
	var injected atomic.Bool
	f := newPoolFixture(t, func(c net.Conn, args []string) bool {
		if args[0] == "GET" && !injected.Swap(true) {
			io.WriteString(c, "$1\r\nf\r\n$1\r\nt\r\n")
			return true
		}
		return false
	})
	c := pooledClient(t, f, testPoolOptions())
	if _, err := c.Get("key"); err != nil {
		t.Fatal(err)
	}
	value, err := c.Get("key")
	if err != nil || string(value) != "f" {
		t.Fatalf("extra reply reused: %q %v", value, err)
	}
	if f.connections.Load() != 2 {
		t.Fatal("connection with buffered extra reply retained")
	}
}
func TestPoolReauthenticatesBeforeCommandsAfterRevocation(t *testing.T) {
	var revoked atomic.Bool
	f := newPoolFixture(t, func(c net.Conn, args []string) bool {
		if args[0] == "AUTH" && revoked.Load() {
			io.WriteString(c, "-WRONGPASS fixture\r\n")
			return true
		}
		return false
	})
	c := pooledClient(t, f, testPoolOptions())
	if _, err := c.Get("key"); err != nil {
		t.Fatal(err)
	}
	revoked.Store(true)
	if _, err := c.Get("key"); err == nil || err.Error() != RedisNoAuth {
		t.Fatalf("revocation was ignored: %v", err)
	}
	if f.gets.Load() != 1 {
		t.Fatal("command sent after failed authentication")
	}
	revoked.Store(false)
	if _, err := c.Get("key"); err != nil {
		t.Fatal(err)
	}
	if f.connections.Load() != 2 {
		t.Fatal("rejected identity's socket was retained")
	}
}
func TestPoolReselectsBeforeCommandsAfterACLRevocation(t *testing.T) {
	var revoked atomic.Bool
	f := newPoolFixture(t, func(c net.Conn, args []string) bool {
		if args[0] == "SELECT" && revoked.Load() {
			io.WriteString(c, "-NOPERM fixture\r\n")
			return true
		}
		return false
	})
	c := pooledClient(t, f, testPoolOptions())
	if _, err := c.Get("key"); err != nil {
		t.Fatal(err)
	}
	revoked.Store(true)
	if _, err := c.Get("key"); err == nil || err.Error() != "redis:noperm" {
		t.Fatalf("SELECT revocation was ignored: %v", err)
	}
	if f.gets.Load() != 1 {
		t.Fatal("GET sent after denied SELECT")
	}
	revoked.Store(false)
	if _, err := c.Get("key"); err != nil {
		t.Fatal(err)
	}
	if f.connections.Load() != 2 {
		t.Fatal("rejected socket retained")
	}
}
func TestPoolRequiresAuthenticationAndExplicitDatabase(t *testing.T) {
	for _, values := range [][3]string{{"", "", "11"}, {"fixture", "password", ""}} {
		var c SimpleRedis
		c.InitWithUsername("127.0.0.1:1", values[0], values[1], values[2])
		if err := c.ConfigurePool(testPoolOptions()); err == nil {
			c.Close()
			t.Fatal("pool accepted implicit identity/database")
		}
	}
}
