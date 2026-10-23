// Package simpleredis provides the small Redis command set used by the Traefik plugin.
package simpleredis

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	RedisUnreachable = "redis:unreachable"
	RedisMiss        = "redis:miss"
	RedisTimeout     = "redis:timeout"
	RedisNoAuth      = "redis:noauth"
	RedisIssue       = "redis:issue?"
	operationTimeout = 1900 * time.Millisecond
	maxReplySize     = 1024 * 1024
)

// SimpleRedis opens one bounded connection per operation. Configure before use.
type SimpleRedis struct {
	host     string
	username string
	pass     string
	database string
}

// Init preserves password-only authentication for existing callers.
func (sr *SimpleRedis) Init(host, pass, database string) {
	sr.InitWithUsername(host, "", pass, database)
}

// InitWithUsername selects an ACL identity; an empty username uses legacy AUTH.
func (sr *SimpleRedis) InitWithUsername(host, username, pass, database string) {
	sr.host, sr.username, sr.pass, sr.database = host, username, pass, database
}

func writeCommand(w *bufio.Writer, args ...string) error {
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, arg := range args {
		if _, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(arg), arg); err != nil {
			return err
		}
	}
	return w.Flush()
}

func readReply(r *bufio.Reader) (byte, []byte, error) {
	line, err := r.ReadSlice('\n')
	if err != nil {
		return 0, nil, err
	}
	if len(line) < 3 || line[len(line)-2] != '\r' {
		return 0, nil, errors.New("redis:invalid reply")
	}
	kind := line[0]
	value := string(line[1 : len(line)-2])
	switch kind {
	case '-':
		// Error messages can contain command arguments; expose only the error class.
		code := strings.SplitN(value, " ", 2)[0]
		if code == "NOAUTH" || code == "WRONGPASS" {
			return 0, nil, errors.New(RedisNoAuth)
		}
		if code == "NOPERM" {
			return 0, nil, errors.New("redis:noperm")
		}
		return 0, nil, errors.New(RedisIssue)
	case '+':
		return kind, []byte(value), nil
	case ':':
		if _, err := strconv.ParseInt(value, 10, 64); err != nil {
			return 0, nil, errors.New("redis:invalid integer")
		}
		return kind, []byte(value), nil
	case '$':
		size, err := strconv.Atoi(value)
		if err != nil || size < -1 || size > maxReplySize {
			return 0, nil, errors.New("redis:invalid bulk length")
		}
		if size == -1 {
			return 0, nil, errors.New(RedisMiss)
		}
		data := make([]byte, size+2)
		if _, err := io.ReadFull(r, data); err != nil {
			return 0, nil, err
		}
		if data[size] != '\r' || data[size+1] != '\n' {
			return 0, nil, errors.New("redis:invalid bulk terminator")
		}
		return kind, data[:size], nil
	default:
		return 0, nil, errors.New("redis:unexpected reply")
	}
}

func exchange(w *bufio.Writer, r *bufio.Reader, expected byte, args ...string) ([]byte, error) {
	if err := writeCommand(w, args...); err != nil {
		return nil, err
	}
	kind, data, err := readReply(r)
	if err != nil {
		if err.Error() == RedisMiss && expected != '$' {
			return nil, errors.New("redis:unexpected reply")
		}
		return nil, err
	}
	if kind != expected || (expected == '+' && string(data) != "OK") ||
		(expected == ':' && string(data) != "0" && string(data) != "1") {
		return nil, errors.New("redis:unexpected reply")
	}
	return data, nil
}

func (sr *SimpleRedis) execute(expected byte, args ...string) ([]byte, error) {
	deadline := time.Now().Add(operationTimeout)
	dialer := net.Dialer{Deadline: deadline}
	conn, err := dialer.Dial("tcp", sr.host)
	if err != nil {
		return nil, errors.New(RedisUnreachable)
	}
	defer conn.Close()
	if err := conn.SetDeadline(deadline); err != nil {
		return nil, err
	}
	data, err := sr.executeConnection(conn, expected, args...)
	if err != nil {
		if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
			return nil, errors.New(RedisTimeout)
		}
	}
	return data, err
}

func (sr *SimpleRedis) executeConnection(conn net.Conn, expected byte, args ...string) ([]byte, error) {
	w, r := bufio.NewWriter(conn), bufio.NewReader(conn)
	if sr.username != "" || sr.pass != "" {
		auth := []string{"AUTH", sr.pass}
		if sr.username != "" {
			auth = []string{"AUTH", sr.username, sr.pass}
		}
		if _, err := exchange(w, r, '+', auth...); err != nil {
			return nil, err
		}
	}
	if sr.database != "" {
		if _, err := exchange(w, r, '+', "SELECT", sr.database); err != nil {
			return nil, err
		}
	}
	return exchange(w, r, expected, args...)
}

func (sr *SimpleRedis) Get(name string) ([]byte, error) { return sr.execute('$', "GET", name) }
func (sr *SimpleRedis) Set(name string, data []byte, duration int64) error {
	_, err := sr.execute('+', "SET", name, string(data), "EX", strconv.FormatInt(duration, 10))
	return err
}
func (sr *SimpleRedis) Del(name string) error {
	_, err := sr.execute(':', "DEL", name)
	return err
}

// Eval runs a bounded script returning a bulk string. Keys remain subject to ACL checks.
func (sr *SimpleRedis) Eval(script string, keys []string, args ...string) (string, error) {
	command := []string{"EVAL", script, strconv.Itoa(len(keys))}
	command = append(command, keys...)
	command = append(command, args...)
	value, err := sr.execute('$', command...)
	return string(value), err
}
