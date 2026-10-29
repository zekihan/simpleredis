package simpleredis

import (
	"bufio"
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

// PoolOptions bounds resources per client identity/database. Pooling is disabled
// until ConfigurePool succeeds. Configure before use.
type PoolOptions struct {
	MaxActive   int
	MaxIdle     int
	MaxWaiters  int
	IdleTimeout time.Duration
	MaxLifetime time.Duration
}

type pooledConnection struct {
	conn      net.Conn
	reader    *bufio.Reader
	writer    *bufio.Writer
	born      time.Time
	idleSince time.Time
}

type connectionPool struct {
	options PoolOptions
	slots   chan struct{}
	waiters chan struct{}
	done    chan struct{}
	stopped chan struct{}
	mu      sync.Mutex
	closed  bool
	idle    []*pooledConnection
	all     map[*pooledConnection]bool
}

// ConfigurePool replaces and closes any previous pool. Init and ConfigurePool
// must not race with operations; Close is safe during operations and idempotent.
func (sr *SimpleRedis) ConfigurePool(options PoolOptions) error {
	if (sr.username == "" && sr.pass == "") || sr.database == "" {
		return errors.New("redis:pool requires authentication and explicit database")
	}

	if options.MaxActive < 1 || options.MaxIdle < 0 || options.MaxIdle > options.MaxActive || options.MaxWaiters < 0 || options.IdleTimeout <= 0 || options.MaxLifetime <= 0 {
		return errors.New("redis:invalid pool options")
	}
	sr.Close()
	p := &connectionPool{options: options, slots: make(chan struct{}, options.MaxActive), waiters: make(chan struct{}, options.MaxWaiters), done: make(chan struct{}), stopped: make(chan struct{}), all: make(map[*pooledConnection]bool)}
	sr.pool = p
	go p.reap()
	return nil
}

// Close retires the pool, closes idle and borrowed sockets and wakes waiters.
// Subsequent operations on this pooled client fail until it is reinitialized.
func (sr *SimpleRedis) Close() {
	if sr.pool == nil {
		return
	}
	p := sr.pool
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.done)
		for c := range p.all {
			c.conn.Close()
			delete(p.all, c)
		}
		p.idle = nil
	}
	p.mu.Unlock()
	<-p.stopped
}

func (p *connectionPool) expired(c *pooledConnection, now time.Time) bool {
	return now.Sub(c.born) >= p.options.MaxLifetime || now.Sub(c.idleSince) >= p.options.IdleTimeout
}

func (p *connectionPool) reap() {
	defer close(p.stopped)
	interval := p.options.IdleTimeout
	if p.options.MaxLifetime < interval {
		interval = p.options.MaxLifetime
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			return
		case now := <-ticker.C:
			p.mu.Lock()
			kept := p.idle[:0]
			for _, c := range p.idle {
				if p.expired(c, now) {
					c.conn.Close()
					delete(p.all, c)
				} else {
					kept = append(kept, c)
				}
			}
			p.idle = kept
			p.mu.Unlock()
		}
	}
}

func (p *connectionPool) acquire(ctx context.Context) error {
	select {
	case <-p.done:
		return errors.New(RedisUnreachable)
	default:
	}
	select {
	case p.slots <- struct{}{}:
		return nil
	default:
	}
	select {
	case p.waiters <- struct{}{}:
		defer func() { <-p.waiters }()
	default:
		return errors.New(RedisUnreachable)
	}
	select {
	case p.slots <- struct{}{}:
		return nil
	case <-p.done:
		return errors.New(RedisUnreachable)
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return errors.New(RedisTimeout)
		}
		return ctx.Err()
	}
}

func (p *connectionPool) take() *pooledConnection {
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.idle) > 0 {
		n := len(p.idle) - 1
		c := p.idle[n]
		p.idle = p.idle[:n]
		if !p.expired(c, time.Now()) {
			return c
		}
		c.conn.Close()
		delete(p.all, c)
	}
	return nil
}

func (p *connectionPool) release(c *pooledConnection, healthy bool) {
	p.mu.Lock()
	if healthy && !p.closed && time.Since(c.born) < p.options.MaxLifetime && len(p.idle) < p.options.MaxIdle {
		c.idleSince = time.Now()
		p.idle = append(p.idle, c)
	} else {
		c.conn.Close()
		delete(p.all, c)
	}
	p.mu.Unlock()
}

func (sr *SimpleRedis) executePooled(ctx context.Context, expected byte, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	p := sr.pool
	if err := p.acquire(ctx); err != nil {
		return nil, err
	}
	defer func() { <-p.slots }()
	if err := ctx.Err(); err != nil {
		if err == context.DeadlineExceeded {
			return nil, errors.New(RedisTimeout)
		}
		return nil, err
	}
	c := p.take()
	if c == nil {
		d := net.Dialer{}
		conn, err := d.DialContext(ctx, "tcp", sr.host)
		if err != nil {
			return nil, errors.New(RedisUnreachable)
		}
		c = &pooledConnection{conn: conn, reader: bufio.NewReader(conn), writer: bufio.NewWriter(conn), born: time.Now()}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			conn.Close()
			return nil, errors.New(RedisUnreachable)
		}
		p.all[c] = true
		p.mu.Unlock()
	}
	healthy := false
	defer func() { p.release(c, healthy) }()
	deadline, _ := ctx.Deadline()
	if err := c.conn.SetDeadline(deadline); err != nil {
		return nil, err
	}
	// Join the cancellation watcher before returning the socket to the pool.
	finished, joined := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(joined)
		select {
		case <-ctx.Done():
			c.conn.Close()
		case <-finished:
		}
	}()
	defer func() {
		close(finished)
		<-joined
		if ctx.Err() != nil {
			healthy = false
		}
	}()
	var data []byte
	var err error
	// Reauthenticate and reselect every checkout to preserve password and
	// command-ACL revocation behavior, including revocation of SELECT.
	err = sr.authenticate(c.writer, c.reader)
	if err == nil {
		err = sr.selectDatabase(c.writer, c.reader)
	}
	if err == nil {
		data, err = exchange(c.writer, c.reader, expected, args...)
	}
	healthy = (err == nil || (err != nil && err.Error() == RedisMiss)) && c.reader.Buffered() == 0
	if ctx.Err() != nil {
		healthy = false
		if ctx.Err() == context.DeadlineExceeded {
			return nil, errors.New(RedisTimeout)
		}
		return nil, ctx.Err()
	}
	if err != nil {
		if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
			return nil, errors.New(RedisTimeout)
		}
	}
	return data, err
}

// GetContext cancels pooled operations while waiting, dialing and reading.
// Unpooled operations retain their legacy deadline after an initial context check.
func (sr *SimpleRedis) GetContext(ctx context.Context, name string) ([]byte, error) {
	if sr.pool == nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return sr.Get(name)
	}
	return sr.executePooled(ctx, '$', "GET", name)
}
