package mux

// ducker: проверка сессий мультиплекса (D-105). Всё своё — в этом файле; в остальных
// файлах правки помечены комментарием `ducker:`.
//
// Что лечим. Мультиплекс держит одно TCP-соединение до сервера и пускает по нему потоки.
// Если соединение умерло молча — оператор забыл его в NAT, пока телефон спал, или
// оболочка телефона заморозила приложение, — клиент об этом не знает. Новый поток h2mux
// открывается «в долг» и ждёт ответа сервера 5 секунд, потом приложение получает ошибку,
// переподключается и попадает в ту же мёртвую сессию. Так до тех пор, пока HTTP/2 сам не
// признает её мёртвой: пинг после 30 секунд тишины и ещё 15 секунд ожидания ответа.
// Снаружи это выглядит как «Телеграм подключается по 10 секунд, через раз».
//
// Что делаем:
//   - сессию, молчавшую дольше probeIdle, перед выдачей новому потоку проверяем пингом
//     с коротким таймаутом, подстроенным под задержку до сервера;
//   - не ответила и пуста — закрываем, поток получит свежую сессию без ошибки;
//   - не ответила, но в ней идут потоки — новых ей не даём и ждём признаков жизни ещё
//     condemnAfter, потом закрываем: висящие потоки обрываются, приложения переподключаются;
//   - признак жизни — любой прочитанный байт, а не только ответ на пинг: при загрузке
//     наш пинг стоит в очереди за данными, и по одному пингу живую сессию не отличить
//     от мёртвой;
//   - при каждом выборе сессии остальные молчащие проверяются в фоне: иначе сессию с
//     висящими потоками, которой клиент новые потоки и не предлагает, никто бы не проверил;
//   - поток h2mux, не получивший ответа сервера за tcpTimeout, запускает ту же проверку;
//   - ResetAll и ProbeAll зовёт приложение: смена сети и включение экрана.

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// healthConfig — пороги проверки. Свои у каждого клиента, чтобы тесты сжимали их, не трогая
// общие.
type healthConfig struct {
	// Сессию, из которой дольше этого ничего не приходило, перед выдачей проверяем. Пока
	// идёт трафик, проверять незачем: живость видна по чтению. HTTP/2 сам пингует после
	// 30 секунд тишины, поэтому проверка нужна в основном после сна.
	probeIdle time.Duration

	// Сколько ждать ответа на проверочный пинг: 4 × сглаженная задержка + 300 мс, в пределах.
	// Пока задержка не измерена — probeDefault.
	probeMin     time.Duration
	probeMax     time.Duration
	probeDefault time.Duration

	// Сессию с потоками, не ответившую на проверку, не рвём сразу: ждём признаков жизни
	// ещё столько. Дольше в мобильной сети живое соединение не молчит.
	condemnAfter time.Duration

	// Как часто, пока ждём приговора, смотреть, не пришло ли что-нибудь.
	condemnPoll time.Duration
}

var defaultHealth = healthConfig{
	probeIdle:    15 * time.Second,
	probeMin:     800 * time.Millisecond,
	probeMax:     2500 * time.Millisecond,
	probeDefault: 1500 * time.Millisecond,
	condemnAfter: 8 * time.Second,
	condemnPoll:  250 * time.Millisecond,
}

// sessionPinger — сессии, которые умеют пинговать: h2mux и yamux. smux не умеет.
type sessionPinger interface {
	ping(ctx context.Context) error
}

// activityConn запоминает, когда из соединения последний раз что-то прочиталось.
// Время — по настенным часам: монотонные у Go во сне телефона стоят, а NAT оператора
// считает настоящее время.
type activityConn struct {
	net.Conn
	lastRead atomic.Int64 // UnixNano
}

func newActivityConn(conn net.Conn) *activityConn {
	c := &activityConn{Conn: conn}
	c.lastRead.Store(time.Now().UnixNano())
	return c
}

func (c *activityConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.lastRead.Store(time.Now().UnixNano())
	}
	return n, err
}

func (c *activityConn) Upstream() any {
	return c.Conn
}

func (c *activityConn) idle() time.Duration {
	return time.Duration(time.Now().UnixNano() - c.lastRead.Load())
}

// checkedSession — сессия клиента с проверкой живости. Клиент кладёт в список только их.
type checkedSession struct {
	abstractSession
	conn   *activityConn
	client *Client

	access  sync.Mutex
	suspect bool         // новых потоков не даём, ждём приговора
	probing *probeResult // идущая проверка, общая для всех, кто её ждёт
}

type probeResult struct {
	done chan struct{}
	err  error
}

func newCheckedSession(session abstractSession, conn *activityConn, client *Client) *checkedSession {
	s := &checkedSession{abstractSession: session, conn: conn, client: client}
	if h2, ok := session.(*h2MuxClientSession); ok {
		h2.onStall = s.stalled
	}
	return s
}

func (s *checkedSession) CanTakeNewRequest() bool {
	return !s.isSuspect() && s.abstractSession.CanTakeNewRequest()
}

func (s *checkedSession) isSuspect() bool {
	s.access.Lock()
	defer s.access.Unlock()
	return s.suspect
}

func (s *checkedSession) needsProbe() bool {
	return s.conn.idle() > s.client.health.probeIdle
}

// verify отвечает, можно ли отдать сессию новому потоку. Зовётся без Client.access:
// проверка ждёт сеть, и держать при этом замок клиента — значит тормозить всех.
func (s *checkedSession) verify() bool {
	pinger, ok := s.abstractSession.(sessionPinger)
	if !ok {
		// smux пинговать не умеет. Пустую сессию дешевле открыть заново, чем гадать;
		// сессию с потоками оставляем, как было в апстриме.
		if s.NumStreams() == 0 {
			s.closeSilent("idle smux session")
			return false
		}
		return true
	}

	mark := s.conn.lastRead.Load()
	err := s.probe(pinger, s.client.probeTimeout())
	if err == nil || s.conn.lastRead.Load() > mark {
		s.clearSuspect()
		return true
	}

	if s.NumStreams() == 0 {
		s.closeSilent("no answer to ping")
	} else {
		s.condemn(pinger, mark)
	}
	return false
}

// stalled — поток h2mux не получил ответа сервера за tcpTimeout. Сервер отвечает на
// открытие потока сразу, так что это почти наверняка мёртвый канал.
func (s *checkedSession) stalled() {
	go s.verify()
}

// probe шлёт пинг; одновременные вызовы ждут один и тот же пинг.
func (s *checkedSession) probe(pinger sessionPinger, timeout time.Duration) error {
	s.access.Lock()
	result := s.probing
	if result == nil {
		result = &probeResult{done: make(chan struct{})}
		s.probing = result
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			start := time.Now()
			silent := s.conn.idle().Round(time.Second)
			result.err = pinger.ping(ctx)
			cancel()
			// В подробном журнале видно, ловила ли проверка мёртвые сессии: по нему и
			// разбираем жалобы «подключается по 10 секунд».
			if result.err == nil {
				rtt := time.Since(start)
				s.client.observeRTT(rtt)
				s.client.logger.Debug("mux: session alive after ", silent, " of silence, ping ", rtt.Round(time.Millisecond))
			} else {
				s.client.logger.Debug("mux: no ping answer in ", timeout, " after ", silent, " of silence")
			}
			s.access.Lock()
			s.probing = nil
			s.access.Unlock()
			close(result.done)
		}()
	}
	s.access.Unlock()
	<-result.done
	return result.err
}

// condemn: новых потоков не даём и ждём признаков жизни condemnAfter. Дождались —
// сессия снова в строю; нет — закрываем, и висящие в ней потоки обрываются сразу, а не
// через полминуты.
func (s *checkedSession) condemn(pinger sessionPinger, mark int64) {
	s.access.Lock()
	if s.suspect {
		s.access.Unlock()
		return
	}
	s.suspect = true
	s.access.Unlock()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), s.client.health.condemnAfter)
		defer cancel()

		answered := make(chan error, 1)
		go func() { answered <- pinger.ping(ctx) }()

		ticker := time.NewTicker(s.client.health.condemnPoll)
		defer ticker.Stop()

		for {
			select {
			case err := <-answered:
				if err == nil || s.conn.lastRead.Load() > mark {
					s.clearSuspect()
					s.client.logger.Debug("mux: silent session came back")
					return
				}
				s.closeSilent("no sign of life with open streams")
				return
			case <-ticker.C:
				if s.conn.lastRead.Load() > mark {
					s.clearSuspect()
					s.client.logger.Debug("mux: silent session came back")
					return
				}
			}
		}
	}()
}

func (s *checkedSession) clearSuspect() {
	s.access.Lock()
	s.suspect = false
	s.access.Unlock()
}

func (s *checkedSession) closeSilent(reason string) {
	if s.IsClosed() {
		return
	}
	s.client.logger.Info("mux: closing dead session (", reason, ", silent for ",
		s.conn.idle().Round(time.Millisecond), ", streams ", s.NumStreams(), ")")
	_ = s.Close()
}

// probeTimeout — сколько ждать ответа на пинг до этого сервера.
func (c *Client) probeTimeout() time.Duration {
	srtt := time.Duration(c.srtt.Load())
	if srtt == 0 {
		return c.health.probeDefault
	}
	timeout := 4*srtt + 300*time.Millisecond
	if timeout < c.health.probeMin {
		return c.health.probeMin
	}
	if timeout > c.health.probeMax {
		return c.health.probeMax
	}
	return timeout
}

// observeRTT — сглаженная задержка до сервера, как у TCP: новое значение весит 1/8.
func (c *Client) observeRTT(rtt time.Duration) {
	old := c.srtt.Load()
	if old == 0 {
		c.srtt.Store(int64(rtt))
		return
	}
	c.srtt.Store(old + (int64(rtt)-old)/8)
}

// probeOthers в фоне проверяет остальные молчащие сессии клиента. Без этого сессия с
// висящими потоками не проверялась бы вовсе: по умолчанию клиент отдаёт новым потокам
// только пустые сессии, а занятую обходит и открывает новую — и мёртвая держала бы свои
// потоки до пинга самого HTTP/2, до 45 секунд.
func (c *Client) probeOthers(chosen abstractSession) {
	for _, s := range c.sessionsSnapshot() {
		if abstractSession(s) != chosen && s.needsProbe() {
			go s.verify()
		}
	}
}

// sessionsSnapshot — живые сессии клиента, без замка на время работы с ними.
func (c *Client) sessionsSnapshot() []*checkedSession {
	c.access.Lock()
	defer c.access.Unlock()
	var sessions []*checkedSession
	for _, session := range c.connections.Array() {
		if checked, ok := session.(*checkedSession); ok && !checked.IsClosed() {
			sessions = append(sessions, checked)
		}
	}
	return sessions
}

// Реестр клиентов: события телефона приходят не к конкретному узлу, а ко всем сразу.
// Устаревшие узлы mihomo закрывает финализатором, и Close убирает клиента отсюда.
var registry = struct {
	sync.Mutex
	clients map[*Client]struct{}
}{clients: make(map[*Client]struct{})}

func register(c *Client) {
	registry.Lock()
	registry.clients[c] = struct{}{}
	registry.Unlock()
}

func unregister(c *Client) {
	registry.Lock()
	delete(registry.clients, c)
	registry.Unlock()
}

func registered() []*Client {
	registry.Lock()
	defer registry.Unlock()
	clients := make([]*Client, 0, len(registry.clients))
	for c := range registry.clients {
		clients = append(clients, c)
	}
	return clients
}

// ResetAll закрывает все сессии мультиплекса: телефон сменил сеть, и каналы через
// прежнюю либо уже мертвы, либо умрут вместе с ней. Следующий поток откроет новую
// сессию через новую сеть.
func ResetAll() {
	for _, c := range registered() {
		c.Reset()
	}
}

// ProbeAll проверяет сессии, молчавшие дольше probeIdle: экран включился, и человек вот-вот
// откроет приложение. К этому моменту мёртвые сессии уже будут закрыты, а живые —
// подтверждены. Не ждёт результата.
func ProbeAll() {
	for _, c := range registered() {
		for _, s := range c.sessionsSnapshot() {
			if s.needsProbe() {
				go s.verify()
			}
		}
	}
}
