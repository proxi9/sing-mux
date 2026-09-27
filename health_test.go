package mux

// ducker: проверка сессий мультиплекса (D-105) на настоящем HTTP/2 и настоящем сервере
// sing-mux. «Мёртвый канал» — соединение, которое после freeze молча глотает всё в обе
// стороны, как соединение, забытое NAT оператора: клиент об этом ничего не знает.

import (
	"context"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/sing/common/logger"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

// fast — пороги, сжатые до долей секунды.
var fast = healthConfig{
	probeIdle:    50 * time.Millisecond,
	probeMin:     100 * time.Millisecond,
	probeMax:     300 * time.Millisecond,
	probeDefault: 200 * time.Millisecond,
	condemnAfter: 500 * time.Millisecond,
	condemnPoll:  20 * time.Millisecond,
}

type echoHandler struct{}

func (echoHandler) NewConnection(ctx context.Context, conn net.Conn, metadata M.Metadata) error {
	_, err := io.Copy(conn, conn)
	return err
}

func (echoHandler) NewPacketConnection(ctx context.Context, conn N.PacketConn, metadata M.Metadata) error {
	return os.ErrInvalid
}

// testNet — сервер sing-mux на локальном порту и дозвон до него, который считает звонки.
type testNet struct {
	listener net.Listener
	dials    atomic.Int32
	access   sync.Mutex
	conns    []*holeConn
}

func newTestNet(t *testing.T) *testNet {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(ServiceOptions{
		NewStreamContext: func(ctx context.Context, conn net.Conn) context.Context { return ctx },
		Logger:           logger.NOP(),
		Handler:          echoHandler{},
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go service.NewConnection(context.Background(), conn, M.Metadata{})
		}
	}()
	t.Cleanup(func() { listener.Close() })
	return &testNet{listener: listener}
}

func (n *testNet) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	n.dials.Add(1)
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", n.listener.Addr().String())
	if err != nil {
		return nil, err
	}
	hole := &holeConn{Conn: conn}
	n.access.Lock()
	n.conns = append(n.conns, hole)
	n.access.Unlock()
	return hole, nil
}

func (n *testNet) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, os.ErrInvalid
}

func (n *testNet) freezeAll() {
	n.access.Lock()
	defer n.access.Unlock()
	for _, c := range n.conns {
		c.frozen.Store(true)
	}
}

// holeConn после freeze глотает запись и выбрасывает пришедшее.
type holeConn struct {
	net.Conn
	frozen atomic.Bool
}

func (c *holeConn) Write(p []byte) (int, error) {
	if c.frozen.Load() {
		return len(p), nil
	}
	return c.Conn.Write(p)
}

func (c *holeConn) Read(p []byte) (int, error) {
	for {
		n, err := c.Conn.Read(p)
		if err != nil || !c.frozen.Load() {
			return n, err
		}
	}
}

func newTestClient(t *testing.T, n *testNet, health healthConfig) *Client {
	client, err := NewClient(Options{
		Dialer:     n,
		Logger:     logger.NOP(),
		Protocol:   "h2mux",
		TCPTimeout: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	client.health = health
	t.Cleanup(func() { client.Close() })
	return client
}

// echo открывает поток, шлёт строку и ждёт её обратно. Возвращает поток открытым.
func echo(client *Client, message string, timeout time.Duration) (net.Conn, error) {
	type result struct {
		conn net.Conn
		err  error
	}
	done := make(chan result, 1)
	go func() {
		conn, err := client.DialContext(context.Background(), N.NetworkTCP, M.ParseSocksaddr("example.com:80"))
		if err != nil {
			done <- result{err: err}
			return
		}
		if _, err = conn.Write([]byte(message)); err != nil {
			conn.Close()
			done <- result{err: err}
			return
		}
		reply := make([]byte, len(message))
		if _, err = io.ReadFull(conn, reply); err != nil {
			conn.Close()
			done <- result{err: err}
			return
		}
		done <- result{conn: conn}
	}()
	select {
	case r := <-done:
		return r.conn, r.err
	case <-time.After(timeout):
		return nil, os.ErrDeadlineExceeded
	}
}

func mustEcho(t *testing.T, client *Client, message string) net.Conn {
	t.Helper()
	conn, err := echo(client, message, 2*time.Second)
	if err != nil {
		t.Fatalf("echo %q: %v", message, err)
	}
	return conn
}

func TestLiveIdleSessionIsReused(t *testing.T) {
	n := newTestNet(t)
	client := newTestClient(t, n, fast)

	mustEcho(t, client, "first").Close()
	time.Sleep(3 * fast.probeIdle) // молчала дольше probeIdle — будет проверка
	mustEcho(t, client, "second").Close()

	if got := n.dials.Load(); got != 1 {
		t.Fatalf("живую сессию заменили: дозвонов %d, ждали 1", got)
	}
}

func TestDeadIdleSessionIsReplacedWithoutError(t *testing.T) {
	n := newTestNet(t)
	client := newTestClient(t, n, fast)

	mustEcho(t, client, "first").Close()
	n.freezeAll()
	time.Sleep(3 * fast.probeIdle)

	start := time.Now()
	mustEcho(t, client, "after sleep").Close()

	if got := n.dials.Load(); got != 2 {
		t.Fatalf("дозвонов %d, ждали 2: мёртвую сессию не заменили", got)
	}
	// Цена мёртвой сессии — один таймаут проверки, а не 5 с ожидания ответа на поток.
	if elapsed := time.Since(start); elapsed > fast.probeMax+time.Second {
		t.Fatalf("замена заняла %v", elapsed)
	}
}

func TestDeadSessionWithStreamsIsClosedSoon(t *testing.T) {
	n := newTestNet(t)
	client := newTestClient(t, n, fast)

	hanging := mustEcho(t, client, "long-lived")
	defer hanging.Close()
	n.freezeAll()
	time.Sleep(3 * fast.probeIdle)

	mustEcho(t, client, "new one").Close()
	if got := n.dials.Load(); got != 2 {
		t.Fatalf("дозвонов %d, ждали 2", got)
	}

	// Поток в мёртвой сессии должен оборваться за condemnAfter, а не висеть 45 с.
	failed := make(chan error, 1)
	go func() {
		_, err := hanging.Read(make([]byte, 1))
		failed <- err
	}()
	select {
	case err := <-failed:
		if err == nil {
			t.Fatal("из мёртвой сессии что-то прочиталось")
		}
	case <-time.After(fast.condemnAfter + 2*time.Second):
		t.Fatal("поток в мёртвой сессии так и висит")
	}
}

func TestResetAllDropsSessions(t *testing.T) {
	n := newTestNet(t)
	client := newTestClient(t, n, fast)

	mustEcho(t, client, "before").Close()
	ResetAll()
	mustEcho(t, client, "after").Close()

	if got := n.dials.Load(); got != 2 {
		t.Fatalf("после смены сети дозвонов %d, ждали 2", got)
	}
}

func TestStalledStreamTriggersCheck(t *testing.T) {
	// Проверка перед выдачей здесь не должна сработать: пусть сессия считается свежей.
	health := fast
	health.probeIdle = time.Second
	n := newTestNet(t)
	client := newTestClient(t, n, health)

	mustEcho(t, client, "warm").Close()
	time.Sleep(50 * time.Millisecond) // закрытый поток ещё мгновение числится активным
	n.freezeAll()

	// Сессия ещё не «молчала» дольше probeIdle, поэтому поток уходит в неё без проверки
	// и не получает ответа за TCPTimeout — это и должно запустить проверку.
	if _, err := echo(client, "stuck", time.Second); err == nil {
		t.Fatal("поток в мёртвой сессии будто бы прошёл")
	}

	deadline := time.Now().Add(fast.probeMax + time.Second)
	for {
		conn, err := echo(client, "recovered", time.Second)
		if err == nil {
			conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("после застрявшего потока сессию так и не заменили: %v", err)
		}
	}
	if got := n.dials.Load(); got < 2 {
		t.Fatalf("дозвонов %d, ждали новую сессию", got)
	}
}
