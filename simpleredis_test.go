package simpleredis

import (
	"bufio"
	"io"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func readCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	count, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "*")))
	if err != nil {
		return nil, err
	}
	var args []string
	for i := 0; i < count; i++ {
		kind, data, err := readReply(r)
		if err != nil {
			return nil, err
		}
		if kind != '$' {
			return nil, io.ErrUnexpectedEOF
		}
		args = append(args, string(data))
	}
	return args, nil
}
func server(t *testing.T, fn func(net.Conn)) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := l.Accept()
		if err == nil {
			defer c.Close()
			_ = c.SetDeadline(time.Now().Add(4 * time.Second))
			fn(c)
		}
	}()
	t.Cleanup(func() { _ = l.Close(); <-done })
	return l.Addr().String()
}
func TestACLAndBinaryArguments(t *testing.T) {
	commands := make(chan [][]string, 1)
	addr := server(t, func(c net.Conn) {
		r := bufio.NewReader(c)
		var got [][]string
		for i := 0; i < 3; i++ {
			args, err := readCommand(r)
			if err != nil {
				return
			}
			got = append(got, args)
			_, _ = io.WriteString(c, "+OK\r\n")
		}
		commands <- got
	})
	var client SimpleRedis
	client.InitWithUsername(addr, "app user", "secret %\r\n", "11")
	if err := client.Set("crowdsec:a b\r\n", []byte("x\x00y"), 60); err != nil {
		t.Fatal(err)
	}
	want := [][]string{{"AUTH", "app user", "secret %\r\n"}, {"SELECT", "11"}, {"SET", "crowdsec:a b\r\n", "x\x00y", "EX", "60"}}
	select {
	case got := <-commands:
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("commands differ")
		}
	case <-time.After(time.Second):
		t.Fatal("server did not finish")
	}
}
func TestErrorsAndReplies(t *testing.T) {
	for _, tc := range []struct{ name, reply, op, want string }{
		{"get denied", "-NOPERM secret details\r\n", "get", "redis:noperm"},
		{"set denied", "-NOPERM secret details\r\n", "set", "redis:noperm"},
		{"delete denied", "-NOPERM secret details\r\n", "del", "redis:noperm"},
		{"missing", "$-1\r\n", "get", RedisMiss},
		{"truncated", "$4\r\nx", "get", "error"},
		{"huge", "$999999999\r\n", "get", "error"},
		{"wrong type", "+OK\r\n", "get", "error"},
		{"deleted", ":0\r\n", "del", ""},
		{"invalid delete count", ":-1\r\n", "del", "error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := server(t, func(c net.Conn) { _, _ = readCommand(bufio.NewReader(c)); _, _ = io.WriteString(c, tc.reply) })
			var client SimpleRedis
			client.Init(addr, "", "")
			var err error
			switch tc.op {
			case "get":
				_, err = client.Get("key")
			case "set":
				err = client.Set("key", []byte("v"), 10)
			case "del":
				err = client.Del("key")
			}
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || (tc.want != "error" && err.Error() != tc.want) {
				t.Fatalf("error = %v, want %s", err, tc.want)
			}
		})
	}
}
func TestAuthenticationFailureStopsCommands(t *testing.T) {
	addr := server(t, func(c net.Conn) {
		r := bufio.NewReader(c)
		args, err := readCommand(r)
		if err != nil || !reflect.DeepEqual(args, []string{"AUTH", "password"}) {
			return
		}
		_, _ = io.WriteString(c, "-WRONGPASS sensitive\r\n")
		if _, err = readCommand(r); err == nil {
			t.Error("command sent after authentication failure")
		}
	})
	var client SimpleRedis
	client.Init(addr, "password", "11")
	if err := client.Del("key"); err == nil || err.Error() != RedisNoAuth {
		t.Fatalf("error = %v", err)
	}
}
func TestOperationDeadline(t *testing.T) {
	addr := server(t, func(c net.Conn) {
		_, _ = readCommand(bufio.NewReader(c))
		for i := 0; i < 30; i++ {
			if _, err := io.WriteString(c, "+"); err != nil {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	})
	var client SimpleRedis
	client.Init(addr, "", "")
	start := time.Now()
	_, err := client.Get("key")
	if err == nil || err.Error() != RedisTimeout {
		t.Fatalf("error = %v", err)
	}
	if time.Since(start) > operationTimeout+250*time.Millisecond {
		t.Fatal("deadline extended by partial reply")
	}
}

func TestSelectFailureStopsCommands(t *testing.T) {
	addr := server(t, func(c net.Conn) {
		reader := bufio.NewReader(c)
		args, err := readCommand(reader)
		if err != nil || !reflect.DeepEqual(args, []string{"SELECT", "11"}) {
			t.Error("expected SELECT first")
			return
		}
		_, _ = io.WriteString(c, "-NOPERM select denied\r\n")
		if _, err = readCommand(reader); err == nil {
			t.Error("command sent after SELECT denial")
		}
	})
	var client SimpleRedis
	client.Init(addr, "", "11")
	if err := client.Set("key", []byte("value"), 60); err == nil || err.Error() != "redis:noperm" {
		t.Fatalf("error = %v", err)
	}
}

func TestBulkValues(t *testing.T) {
	for _, value := range []string{"", "a\x00b\r\nc% value"} {
		addr := server(t, func(c net.Conn) {
			_, _ = readCommand(bufio.NewReader(c))
			reply := "$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n"
			for _, b := range []byte(reply) {
				_, _ = c.Write([]byte{b})
			}
		})
		var client SimpleRedis
		client.Init(addr, "", "")
		got, err := client.Get("key")
		if err != nil || string(got) != value {
			t.Fatalf("bulk value mismatch: %v", err)
		}
	}
}

func TestNullAuthenticationIsNotCacheMiss(t *testing.T) {
	addr := server(t, func(c net.Conn) {
		_, _ = readCommand(bufio.NewReader(c))
		_, _ = io.WriteString(c, "$-1\r\n")
	})
	var client SimpleRedis
	client.InitWithUsername(addr, "fixture", "fixture", "11")
	_, err := client.Get("crowdsec:key")
	if err == nil || err.Error() == RedisMiss {
		t.Fatalf("malformed AUTH reply classified as a miss: %v", err)
	}
}

func TestEvalEncodingAndReply(t *testing.T) {
	observed := make(chan []string, 1)
	addr := server(t, func(c net.Conn) {
		args, err := readCommand(bufio.NewReader(c))
		if err != nil {
			return
		}
		observed <- args
		_, _ = io.WriteString(c, "$2\r\nOK\r\n")
	})
	var client SimpleRedis
	client.Init(addr, "", "")
	got, err := client.Eval("return ARGV[1]", []string{"prefix:lease"}, "value with spaces")
	if err != nil || got != "OK" {
		t.Fatal(got, err)
	}
	want := []string{"EVAL", "return ARGV[1]", "1", "prefix:lease", "value with spaces"}
	if args := <-observed; !reflect.DeepEqual(args, want) {
		t.Fatal(args)
	}
}
