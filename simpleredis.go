// Package simpleredis implements utility routines for interacting with redis.
// It supports currently the following operations: GET, SET, DELETE,
// and support timetoleave for keys.
// Connections are pooled: a command reuses an already authenticated connection
// when one is idle, and only opens a new one when none is available.
// With NO external dependencies.
package simpleredis

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

// Error strings for redis.
const (
	RedisUnreachable = "redis:unreachable"
	RedisMiss        = "redis:miss"
	RedisTimeout     = "redis:timeout"
	RedisNoAuth      = "redis:noauth"
	RedisIssue       = "redis:issue?"
)

const (
	maxIdleConns = 8
	idleTimeout  = 30 * time.Second
	dialTimeout  = 2 * time.Second
	ioTimeout    = 1 * time.Second
)

//nolint:gochecknoglobals
var (
	errUnreachable = errors.New(RedisUnreachable)
	errMiss        = errors.New(RedisMiss)
	errTimeout     = errors.New(RedisTimeout)
	errNoAuth      = errors.New(RedisNoAuth)
	errIssue       = errors.New(RedisIssue)
)

// A pooledConn is a connection to redis on which AUTH and SELECT already ran.
type pooledConn struct {
	netConn  net.Conn
	reader   *bufio.Reader
	writer   *bufio.Writer
	lastUsed time.Time
}

func (c *pooledConn) close() {
	_ = c.netConn.Close()
}

// A SimpleRedis is used to communicate with redis. Its zero value is unusable,
// call Init first. It is safe for concurrent use and must not be copied.
type SimpleRedis struct {
	host     string
	pass     string
	database string

	mu   sync.Mutex
	idle []*pooledConn
}

// Init sets the redisHost used to connect to redis.
func (sr *SimpleRedis) Init(host, pass, database string) {
	sr.host = host
	sr.pass = pass
	sr.database = database
}

// Get fetches the value for key name in redis.
func (sr *SimpleRedis) Get(name string) ([]byte, error) {
	return sr.exec([]byte("GET"), []byte(name))
}

// Set updates the value for key name in redis with value data for duration seconds.
func (sr *SimpleRedis) Set(name string, data []byte, duration int64) error {
	_, err := sr.exec([]byte("SET"), []byte(name), data, []byte("EX"), []byte(strconv.FormatInt(duration, 10)))
	return err
}

// Del removes the key name in redis.
func (sr *SimpleRedis) Del(name string) error {
	_, err := sr.exec([]byte("DEL"), []byte(name))
	return err
}

// exec runs one command, on a pooled connection when one is available.
func (sr *SimpleRedis) exec(args ...[]byte) ([]byte, error) {
	conn, reused, err := sr.borrow()
	if err != nil {
		return nil, err
	}
	data, err, reusable := sr.do(conn, args)
	sr.release(conn, reusable)
	if err == nil || reusable || !reused {
		return data, err
	}
	// A pooled connection died between two commands, which redis closing idle
	// clients, a restart or a firewall all do, and nothing tells us before we
	// write on it. Run the command once more, on a connection known to be fresh.
	conn, err = sr.dial()
	if err != nil {
		return nil, err
	}
	data, err, reusable = sr.do(conn, args)
	sr.release(conn, reusable)
	return data, err
}

// borrow takes an idle connection, or opens one. It reports whether the
// connection comes from the pool, as only those can be stale.
func (sr *SimpleRedis) borrow() (*pooledConn, bool, error) {
	var reused *pooledConn
	var stale []*pooledConn
	now := time.Now()

	sr.mu.Lock()
	for len(sr.idle) > 0 {
		conn := sr.idle[len(sr.idle)-1]
		sr.idle = sr.idle[:len(sr.idle)-1]
		if now.Sub(conn.lastUsed) < idleTimeout {
			reused = conn
			break
		}
		stale = append(stale, conn)
	}
	sr.mu.Unlock()

	for _, conn := range stale {
		conn.close()
	}
	if reused != nil {
		return reused, true, nil
	}
	conn, err := sr.dial()
	return conn, false, err
}

// release returns a connection to the pool, or closes it when it cannot be reused.
func (sr *SimpleRedis) release(conn *pooledConn, reusable bool) {
	if !reusable {
		conn.close()
		return
	}
	conn.lastUsed = time.Now()

	sr.mu.Lock()
	if len(sr.idle) >= maxIdleConns {
		sr.mu.Unlock()
		conn.close()
		return
	}
	sr.idle = append(sr.idle, conn)
	sr.mu.Unlock()
}

// dial opens a connection and runs AUTH and SELECT on it, once for its whole life.
func (sr *SimpleRedis) dial() (*pooledConn, error) {
	dialer := net.Dialer{Timeout: dialTimeout}
	netConn, err := dialer.Dial("tcp", sr.host)
	if err != nil {
		return nil, errUnreachable
	}
	conn := &pooledConn{
		netConn: netConn,
		reader:  bufio.NewReader(netConn),
		writer:  bufio.NewWriter(netConn),
	}

	if sr.pass != "" {
		if _, err, _ = sr.do(conn, [][]byte{[]byte("AUTH"), []byte(sr.pass)}); err != nil {
			conn.close()
			return nil, err
		}
	}
	if sr.database != "" {
		if _, err, _ = sr.do(conn, [][]byte{[]byte("SELECT"), []byte(sr.database)}); err != nil {
			conn.close()
			return nil, err
		}
	}
	return conn, nil
}

// do sends one command and reads its reply. It also reports whether the
// connection is left in a known state and can go back in the pool.
func (sr *SimpleRedis) do(conn *pooledConn, args [][]byte) ([]byte, error, bool) { //nolint:revive
	if err := conn.netConn.SetDeadline(time.Now().Add(ioTimeout)); err != nil {
		return nil, errUnreachable, false
	}
	if err := writeCommand(conn.writer, args); err != nil {
		return nil, ioError(err), false
	}
	data, err, clean := readReply(conn.reader)
	if err != nil && !clean {
		return nil, ioError(err), false
	}
	return data, err, true
}

// writeCommand sends a command as a RESP array, which unlike the inline
// commands keeps keys and values binary safe whatever spaces they hold.
func writeCommand(writer *bufio.Writer, args [][]byte) error {
	if _, err := writer.WriteString("*" + strconv.Itoa(len(args)) + "\r\n"); err != nil {
		return err
	}
	for _, arg := range args {
		if _, err := writer.WriteString("$" + strconv.Itoa(len(arg)) + "\r\n"); err != nil {
			return err
		}
		if _, err := writer.Write(arg); err != nil {
			return err
		}
		if _, err := writer.WriteString("\r\n"); err != nil {
			return err
		}
	}
	return writer.Flush()
}

// readReply reads one RESP reply. It reports whether the connection is left
// clean, which an error reply or a missing key are but a broken or unexpected
// stream is not.
func readReply(reader *bufio.Reader) ([]byte, error, bool) { //nolint:revive
	line, err := readLine(reader)
	if err != nil {
		return nil, err, false
	}
	if len(line) == 0 {
		return nil, errIssue, false
	}

	switch line[0] {
	case '+', ':':
		return line[1:], nil, true
	case '-':
		return nil, replyError(line[1:]), true
	case '$':
		length, convErr := strconv.Atoi(string(line[1:]))
		if convErr != nil {
			return nil, errIssue, false
		}
		if length < 0 {
			return nil, errMiss, true
		}
		// The value can hold anything, CRLF included, so read exactly its
		// announced length rather than up to the next line break.
		data := make([]byte, length+2)
		if _, err = io.ReadFull(reader, data); err != nil {
			return nil, err, false
		}
		return data[:length], nil, true
	default:
		return nil, errIssue, false
	}
}

// readLine reads one CRLF terminated protocol line and returns it without the CRLF.
func readLine(reader *bufio.Reader) ([]byte, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, errIssue
	}
	return line[:len(line)-2], nil
}

// replyError maps an error reply from redis, keeping its message for anything
// the caller cannot act on anyway.
func replyError(message []byte) error {
	for _, prefix := range [][]byte{[]byte("NOAUTH"), []byte("WRONGPASS"), []byte("NOPERM"), []byte("ERR Client sent AUTH")} {
		if bytes.HasPrefix(message, prefix) {
			return errNoAuth
		}
	}
	return errors.New(string(message))
}

// ioError tells a connection that timed out from one that broke.
func ioError(err error) error {
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() { //nolint:errorlint
		return errTimeout
	}
	return errUnreachable
}
