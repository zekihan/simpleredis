package simpleredis

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeRedis is a redis speaking just enough RESP to exercise the client.
type fakeRedis struct {
	listener net.Listener
	pass     string

	mu       sync.Mutex
	values   map[string]string
	commands []string // every command received, across all connections

	conns atomic.Int64 // connections accepted since the server started

	// closeAfter closes a connection after that many commands, 0 to never close.
	closeAfter int64
	// silent accepts connections but never answers.
	silent bool
}

func newFakeRedis(t *testing.T) *fakeRedis {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := &fakeRedis{listener: listener, values: map[string]string{}}
	go server.serve()
	t.Cleanup(func() { _ = listener.Close() })
	return server
}

func (s *fakeRedis) addr() string { return s.listener.Addr().String() }

func (s *fakeRedis) serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.conns.Add(1)
		go s.handle(conn)
	}
}

func (s *fakeRedis) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	reader := bufio.NewReader(conn)
	var served int64
	for {
		args, err := readCommand(reader)
		if err != nil {
			return
		}
		s.mu.Lock()
		s.commands = append(s.commands, strings.ToUpper(args[0]))
		s.mu.Unlock()
		if s.silent {
			continue
		}
		if _, err = conn.Write([]byte(s.reply(args))); err != nil {
			return
		}
		served++
		if s.closeAfter > 0 && served >= s.closeAfter {
			return
		}
	}
}

func (s *fakeRedis) reply(args []string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch strings.ToUpper(args[0]) {
	case "AUTH":
		if s.pass != "" && args[1] != s.pass {
			return "-WRONGPASS invalid username-password pair\r\n"
		}
		return "+OK\r\n"
	case "SELECT":
		return "+OK\r\n"
	case "SET":
		s.values[args[1]] = args[2]
		return "+OK\r\n"
	case "DEL":
		_, found := s.values[args[1]]
		delete(s.values, args[1])
		if found {
			return ":1\r\n"
		}
		return ":0\r\n"
	case "GET":
		value, found := s.values[args[1]]
		if !found {
			return "$-1\r\n"
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
	default:
		return "-ERR unknown command\r\n"
	}
}

func (s *fakeRedis) commandsSeen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.commands...)
}

// readCommand reads one RESP array, which is the only form the client sends.
func readCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 4 || line[0] != '*' {
		return nil, fmt.Errorf("not a RESP array: %q", line)
	}
	count, err := strconv.Atoi(strings.TrimSuffix(line, "\r\n")[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, count)
	for i := 0; i < count; i++ {
		if line, err = reader.ReadString('\n'); err != nil {
			return nil, err
		}
		if len(line) < 4 || line[0] != '$' {
			return nil, fmt.Errorf("not a RESP bulk string: %q", line)
		}
		length, convErr := strconv.Atoi(strings.TrimSuffix(line, "\r\n")[1:])
		if convErr != nil {
			return nil, convErr
		}
		buf := make([]byte, length+2)
		if _, err = io.ReadFull(reader, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:length]))
	}
	return args, nil
}

func Test_GetSetDel(t *testing.T) {
	server := newFakeRedis(t)
	var redis SimpleRedis
	redis.Init(server.addr(), "", "")
	defer redis.Close()

	if _, err := redis.Get("absent"); err == nil || err.Error() != RedisMiss {
		t.Fatalf("Get on an absent key = %v, want %v", err, RedisMiss)
	}
	if err := redis.Set("key", []byte("value"), 60); err != nil {
		t.Fatalf("Set() = %v, want nil", err)
	}
	value, err := redis.Get("key")
	if err != nil || string(value) != "value" {
		t.Fatalf("Get() = %q, %v, want %q, nil", value, err, "value")
	}
	if err = redis.Del("key"); err != nil {
		t.Fatalf("Del() = %v, want nil", err)
	}
	if _, err = redis.Get("key"); err == nil || err.Error() != RedisMiss {
		t.Fatalf("Get after Del = %v, want %v", err, RedisMiss)
	}
}

// Values holding spaces or CRLF used to be mangled by the inline protocol.
func Test_BinarySafeValues(t *testing.T) {
	server := newFakeRedis(t)
	var redis SimpleRedis
	redis.Init(server.addr(), "", "")
	defer redis.Close()

	for _, value := range []string{"a value with spaces", "with\r\na newline", "", "24,25"} {
		if err := redis.Set("key", []byte(value), 60); err != nil {
			t.Fatalf("Set(%q) = %v, want nil", value, err)
		}
		got, err := redis.Get("key")
		if err != nil {
			t.Fatalf("Get after Set(%q) = %v, want nil", value, err)
		}
		if string(got) != value {
			t.Errorf("Get() = %q, want %q", got, value)
		}
	}
}

func Test_ConnectionIsReused(t *testing.T) {
	server := newFakeRedis(t)
	server.pass = "secret"
	var redis SimpleRedis
	redis.Init(server.addr(), "secret", "1")
	defer redis.Close()

	for i := 0; i < 50; i++ {
		if _, err := redis.Get("key"); err == nil || err.Error() != RedisMiss {
			t.Fatalf("Get %d = %v, want %v", i, err, RedisMiss)
		}
	}
	if got := server.conns.Load(); got != 1 {
		t.Errorf("connections opened = %d, want 1", got)
	}

	// AUTH and SELECT are paid once for the whole connection, not once per command.
	var auths, selects int
	for _, command := range server.commandsSeen() {
		switch command {
		case "AUTH":
			auths++
		case "SELECT":
			selects++
		}
	}
	if auths != 1 || selects != 1 {
		t.Errorf("AUTH sent %d times and SELECT %d times, want 1 and 1", auths, selects)
	}
}

// A pooled connection dropped on the server side must not surface as an error.
func Test_StalePooledConnectionIsRetried(t *testing.T) {
	server := newFakeRedis(t)
	server.closeAfter = 1
	var redis SimpleRedis
	redis.Init(server.addr(), "", "")
	defer redis.Close()

	for i := 0; i < 5; i++ {
		if _, err := redis.Get("key"); err == nil || err.Error() != RedisMiss {
			t.Fatalf("Get %d = %v, want %v", i, err, RedisMiss)
		}
	}
}

// A connection idle for too long is dropped rather than handed out.
func Test_IdleTimeout(t *testing.T) {
	server := newFakeRedis(t)
	var redis SimpleRedis
	redis.InitWithOptions(server.addr(), "", "", Options{IdleTimeout: 20 * time.Millisecond})
	defer redis.Close()

	if _, err := redis.Get("key"); err == nil || err.Error() != RedisMiss {
		t.Fatalf("Get() = %v, want %v", err, RedisMiss)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := redis.Get("key"); err == nil || err.Error() != RedisMiss {
		t.Fatalf("Get() = %v, want %v", err, RedisMiss)
	}
	if got := server.conns.Load(); got != 2 {
		t.Errorf("connections opened = %d, want 2", got)
	}
}

func Test_MaxIdleConns(t *testing.T) {
	server := newFakeRedis(t)
	var redis SimpleRedis
	redis.InitWithOptions(server.addr(), "", "", Options{MaxIdleConns: 2})
	defer redis.Close()

	release := make(chan struct{})
	var started, done sync.WaitGroup
	for i := 0; i < 5; i++ {
		started.Add(1)
		done.Add(1)
		go func() {
			defer done.Done()
			conn, _, err := redis.borrow()
			started.Done()
			if err != nil {
				t.Errorf("borrow() = %v, want nil", err)
				return
			}
			<-release
			redis.release(conn, true)
		}()
	}
	started.Wait()
	close(release)
	done.Wait()

	redis.mu.Lock()
	pooled := len(redis.idle)
	redis.mu.Unlock()
	if pooled != 2 {
		t.Errorf("pooled connections = %d, want 2", pooled)
	}
}

// A wrong password must report it instead of blocking the caller forever.
func Test_BadAuthDoesNotHang(t *testing.T) {
	server := newFakeRedis(t)
	server.pass = "secret"
	var redis SimpleRedis
	redis.Init(server.addr(), "wrong", "")
	defer redis.Close()

	done := make(chan error, 1)
	go func() {
		_, err := redis.Get("key")
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || err.Error() != RedisNoAuth {
			t.Fatalf("Get() = %v, want %v", err, RedisNoAuth)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Get() blocked on a rejected AUTH")
	}
}

// A redis that accepts the connection but never answers must time out.
func Test_TimeoutIsEnforced(t *testing.T) {
	server := newFakeRedis(t)
	server.silent = true
	var redis SimpleRedis
	redis.InitWithOptions(server.addr(), "", "", Options{IOTimeout: 100 * time.Millisecond})
	defer redis.Close()

	start := time.Now()
	_, err := redis.Get("key")
	if err == nil || err.Error() != RedisTimeout {
		t.Fatalf("Get() = %v, want %v", err, RedisTimeout)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Get() took %v, want it to give up on the deadline", elapsed)
	}
}

func Test_Unreachable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()

	var redis SimpleRedis
	redis.Init(addr, "", "")
	if _, err = redis.Get("key"); err == nil || err.Error() != RedisUnreachable {
		t.Fatalf("Get() = %v, want %v", err, RedisUnreachable)
	}
	if err = redis.Set("key", []byte("value"), 60); err == nil || err.Error() != RedisUnreachable {
		t.Fatalf("Set() = %v, want %v", err, RedisUnreachable)
	}
}

func Test_Concurrent(t *testing.T) {
	server := newFakeRedis(t)
	var redis SimpleRedis
	redis.Init(server.addr(), "", "")
	defer redis.Close()

	var group sync.WaitGroup
	for i := 0; i < 32; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			key := "key" + strconv.Itoa(i)
			for j := 0; j < 20; j++ {
				if err := redis.Set(key, []byte(key), 60); err != nil {
					t.Errorf("Set() = %v, want nil", err)
					return
				}
				value, err := redis.Get(key)
				if err != nil || string(value) != key {
					t.Errorf("Get() = %q, %v, want %q, nil", value, err, key)
					return
				}
			}
		}(i)
	}
	group.Wait()
}
