// Package server accepts TCP connections and runs the work protocol.
package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/thadeu/anway/internal/config"
	"github.com/thadeu/anway/internal/job"
	"github.com/thadeu/anway/internal/manager"
	"github.com/thadeu/anway/internal/protocol"
	"github.com/thadeu/anway/internal/storage"
)

const (
	protocolVersion  = 2
	hashIterations   = 5000
	handshakeTimeout = 10 * time.Second
	writeTimeout     = 30 * time.Second
)

// Options holds the settings that change what clients can do.
type Options struct {
	Password   string
	AllowFlush bool
}

type Server struct {
	manager    *manager.Manager
	password   string
	allowFlush bool
	log        *slog.Logger
	started    time.Time

	mu    sync.Mutex
	conns map[net.Conn]struct{}
	wg    sync.WaitGroup
}

func New(m *manager.Manager, options Options, log *slog.Logger) *Server {
	return &Server{
		manager:    m,
		password:   options.Password,
		allowFlush: options.AllowFlush,
		log:        log,
		started:    time.Now(),
		conns:      map[net.Conn]struct{}{},
	}
}

// Serve accepts connections until the listener closes.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			return nil
		}

		if err != nil {
			return err
		}

		s.track(conn)
		s.wg.Add(1)

		go func() {
			defer s.wg.Done()
			defer s.untrack(conn)

			s.handle(ctx, conn)
		}()
	}
}

// Shutdown sends quiet, then terminate, and closes the connections that remain.
func (s *Server) Shutdown(timeout time.Duration) {
	s.manager.SetState(manager.StateQuiet)
	s.waitForConnections(timeout / 2)

	s.manager.SetState(manager.StateTerminate)
	s.waitForConnections(timeout / 2)

	s.mu.Lock()

	for conn := range s.conns {
		conn.Close()
	}

	s.mu.Unlock()
	s.wg.Wait()
}

func (s *Server) waitForConnections(timeout time.Duration) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		s.mu.Lock()
		open := len(s.conns)
		s.mu.Unlock()

		if open == 0 {
			return
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (s *Server) connectionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.conns)
}

func (s *Server) track(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.conns[conn] = struct{}{}
}

func (s *Server) untrack(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.conns, conn)
	conn.Close()
}

type session struct {
	conn   net.Conn
	reader *bufio.Reader
	wid    string
}

type hello struct {
	Version  int    `json:"v"`
	PwdHash  string `json:"pwdhash"`
	Hostname string `json:"hostname"`
	WID      string `json:"wid"`
}

func (s *Server) handle(ctx context.Context, conn net.Conn) {
	current := &session{conn: conn, reader: bufio.NewReader(conn)}

	conn.SetDeadline(time.Now().Add(handshakeTimeout))

	if err := s.handshake(current); err != nil {
		s.log.Debug("handshake failed", "error", err)

		return
	}

	for {
		conn.SetDeadline(time.Time{})

		line, err := protocol.ReadLine(current.reader)
		if err != nil {
			return
		}

		conn.SetWriteDeadline(time.Now().Add(writeTimeout))

		if done := s.dispatch(ctx, current, line); done {
			return
		}
	}
}

func (s *Server) handshake(current *session) error {
	greeting := map[string]any{"v": protocolVersion}
	salt := ""

	if s.password != "" {
		raw := make([]byte, 8)
		rand.Read(raw)

		salt = hex.EncodeToString(raw)
		greeting["s"] = salt
		greeting["i"] = hashIterations
	}

	encoded, err := json.Marshal(greeting)
	if err != nil {
		return err
	}

	if err := protocol.WriteSimple(current.conn, "HI "+string(encoded)); err != nil {
		return err
	}

	line, err := protocol.ReadLine(current.reader)
	if err != nil {
		return err
	}

	verb, args := protocol.SplitCommand(line)

	var request hello

	if verb != "HELLO" || json.Unmarshal(args, &request) != nil {
		protocol.WriteError(current.conn, "expected HELLO")

		return errors.New("expected HELLO")
	}

	if request.Version != protocolVersion {
		protocol.WriteError(current.conn, "unsupported protocol version")

		return errors.New("unsupported protocol version")
	}

	if s.password != "" {
		expected := protocol.PasswordHash(s.password, salt, hashIterations)

		if subtle.ConstantTimeCompare([]byte(expected), []byte(request.PwdHash)) != 1 {
			protocol.WriteError(current.conn, "invalid password")

			return errors.New("invalid password")
		}
	}

	current.wid = request.WID

	if current.wid != "" {
		s.manager.Beat(current.wid)
	}

	return protocol.WriteSimple(current.conn, "OK")
}

// dispatch runs one command. It returns true when the connection must close.
func (s *Server) dispatch(ctx context.Context, current *session, line []byte) bool {
	verb, args := protocol.SplitCommand(line)
	conn := current.conn

	var err error

	switch verb {
	case "PUSH":
		err = s.push(conn, args)
	case "FETCH":
		err = s.fetch(ctx, current, args)
	case "ACK":
		err = s.ack(conn, args)
	case "FAIL":
		err = s.fail(conn, args)
	case "BEAT":
		err = s.beat(current, args)
	case "INFO":
		err = s.info(conn)
	case "FLUSH":
		err = s.flush(conn)
	case "QUEUE":
		err = s.queue(conn, args)
	case "BATCH":
		err = s.batch(conn, args)
	case "TRACK":
		err = s.trackCommand(conn, args)
	case "MUTATE":
		err = s.mutate(conn, args)
	case "END":
		protocol.WriteSimple(conn, "OK")

		return true
	default:
		err = fmt.Errorf("unknown command %q", verb)
	}

	if errors.Is(err, storage.ErrNotUnique) {
		return protocol.WriteCoded(conn, "NOTUNIQUE", "Job not unique") != nil
	}

	if err != nil {
		if protocol.WriteError(conn, sanitize(err)) != nil {
			return true
		}
	}

	return false
}

func (s *Server) push(conn net.Conn, args []byte) error {
	var pushed job.Job

	if err := json.Unmarshal(args, &pushed); err != nil {
		return fmt.Errorf("invalid job: %w", err)
	}

	if err := s.manager.Push(&pushed); err != nil {
		return err
	}

	return protocol.WriteSimple(conn, "OK")
}

func (s *Server) fetch(ctx context.Context, current *session, args []byte) error {
	if current.wid == "" {
		return errors.New("FETCH needs a worker connection")
	}

	queues := strings.Fields(string(args))
	if len(queues) == 0 {
		queues = []string{job.DefaultQueue}
	}

	for _, queue := range queues {
		if !job.ValidQueue(queue) {
			return fmt.Errorf("invalid queue name: %q", queue)
		}
	}

	fetched, err := s.manager.Fetch(ctx, queues, current.wid)
	if err != nil {
		return err
	}

	current.conn.SetWriteDeadline(time.Now().Add(writeTimeout))

	if fetched == nil {
		return protocol.WriteBulk(current.conn, nil)
	}

	encoded, err := json.Marshal(fetched)
	if err != nil {
		return err
	}

	return protocol.WriteBulk(current.conn, encoded)
}

func (s *Server) queue(conn net.Conn, args []byte) error {
	fields := strings.Fields(string(args))
	if len(fields) < 2 {
		return errors.New("QUEUE needs an action and queue names")
	}

	action, names := fields[0], fields[1:]

	for _, name := range names {
		if name != "*" && !job.ValidQueue(name) {
			return fmt.Errorf("invalid queue name: %q", name)
		}
	}

	var err error

	switch action {
	case "PAUSE":
		err = s.manager.PauseQueues(names)
	case "RESUME":
		err = s.manager.ResumeQueues(names)
	case "REMOVE":
		err = s.manager.RemoveQueues(names)
	default:
		err = fmt.Errorf("unknown QUEUE action %q", action)
	}

	if err != nil {
		return err
	}

	return protocol.WriteSimple(conn, "OK")
}

func (s *Server) mutate(conn net.Conn, args []byte) error {
	var request struct {
		Command string `json:"cmd"`
		Target  string `json:"target"`
		Filter  struct {
			JobType string   `json:"jobtype"`
			JIDs    []string `json:"jids"`
			Regexp  string   `json:"regexp"`
		} `json:"filter"`
	}

	if err := json.Unmarshal(args, &request); err != nil {
		return fmt.Errorf("invalid MUTATE: %w", err)
	}

	if !slices.Contains([]string{"kill", "discard", "requeue", "clear"}, request.Command) {
		return fmt.Errorf("invalid cmd: %q", request.Command)
	}

	if !slices.Contains([]string{"retries", "scheduled", "dead"}, request.Target) {
		return fmt.Errorf("invalid target: %q", request.Target)
	}

	pattern, err := globPattern(request.Filter.Regexp)
	if err != nil {
		return err
	}

	match := func(j *job.Job, encoded []byte) bool {
		if request.Filter.JobType != "" && request.Filter.JobType != j.Type {
			return false
		}

		if len(request.Filter.JIDs) > 0 && !slices.Contains(request.Filter.JIDs, j.JID) {
			return false
		}

		return pattern.Match(encoded)
	}

	mutation := storage.Mutation{Command: request.Command, Target: request.Target, Match: match}

	if err := s.manager.Mutate(mutation); err != nil {
		return err
	}

	return protocol.WriteSimple(conn, "OK")
}

// globPattern accepts the "*" and "?" wildcards. A blank pattern matches all.
func globPattern(glob string) (*regexp.Regexp, error) {
	if glob == "" {
		glob = "*"
	}

	quoted := regexp.QuoteMeta(glob)
	quoted = strings.ReplaceAll(quoted, `\*`, ".*")
	quoted = strings.ReplaceAll(quoted, `\?`, ".")

	return regexp.Compile("(?s)^" + quoted + "$")
}

func (s *Server) batch(conn net.Conn, args []byte) error {
	action, rest, _ := bytes.Cut(args, []byte(" "))
	bid := strings.TrimSpace(string(rest))

	switch string(action) {
	case "NEW":
		var definition storage.BatchDefinition

		if err := json.Unmarshal(rest, &definition); err != nil {
			return fmt.Errorf("invalid BATCH NEW: %w", err)
		}

		created, err := s.manager.BatchNew(definition)
		if err != nil {
			return err
		}

		return protocol.WriteBulk(conn, []byte(created))
	case "OPEN":
		if err := s.manager.BatchOpen(bid); err != nil {
			return err
		}

		return protocol.WriteBulk(conn, []byte(bid))
	case "COMMIT":
		if err := s.manager.BatchCommit(bid); err != nil {
			return err
		}

		return protocol.WriteSimple(conn, "OK")
	case "STATUS":
		status, err := s.manager.BatchStatus(bid)
		if err != nil {
			return err
		}

		return writeJSON(conn, status)
	default:
		return fmt.Errorf("unknown BATCH action %q", action)
	}
}

func (s *Server) trackCommand(conn net.Conn, args []byte) error {
	action, rest, _ := bytes.Cut(args, []byte(" "))

	switch string(action) {
	case "GET":
		track, err := s.manager.TrackGet(strings.TrimSpace(string(rest)))
		if err != nil {
			return err
		}

		return writeJSON(conn, track)
	case "SET":
		var update storage.TrackUpdate

		if err := json.Unmarshal(rest, &update); err != nil {
			return fmt.Errorf("invalid TRACK SET: %w", err)
		}

		if err := s.manager.TrackSet(update); err != nil {
			return err
		}

		return protocol.WriteSimple(conn, "OK")
	default:
		return fmt.Errorf("unknown TRACK action %q", action)
	}
}

func writeJSON(conn net.Conn, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return protocol.WriteBulk(conn, encoded)
}

func (s *Server) ack(conn net.Conn, args []byte) error {
	var request struct {
		JID string `json:"jid"`
	}

	if err := json.Unmarshal(args, &request); err != nil {
		return fmt.Errorf("invalid ACK: %w", err)
	}

	if err := s.manager.Ack(request.JID); err != nil {
		return err
	}

	return protocol.WriteSimple(conn, "OK")
}

func (s *Server) fail(conn net.Conn, args []byte) error {
	var report manager.FailReport

	if err := json.Unmarshal(args, &report); err != nil {
		return fmt.Errorf("invalid FAIL: %w", err)
	}

	if err := s.manager.Fail(report); err != nil {
		return err
	}

	return protocol.WriteSimple(conn, "OK")
}

func (s *Server) beat(current *session, args []byte) error {
	var request struct {
		WID string `json:"wid"`
	}

	if err := json.Unmarshal(args, &request); err != nil {
		return fmt.Errorf("invalid BEAT: %w", err)
	}

	if request.WID == "" || request.WID != current.wid {
		return errors.New("BEAT wid does not match this connection")
	}

	state := s.manager.Beat(request.WID)
	if state == manager.StateRunning {
		return protocol.WriteSimple(current.conn, "OK")
	}

	encoded, err := json.Marshal(map[string]string{"state": state})
	if err != nil {
		return err
	}

	return protocol.WriteBulk(current.conn, encoded)
}

func (s *Server) info(conn net.Conn) error {
	stats, err := s.manager.Stats()
	if err != nil {
		return err
	}

	var enqueued uint64

	for _, size := range stats.Queues {
		enqueued += size
	}

	now := time.Now().UTC()

	encoded, err := json.Marshal(map[string]any{
		"now":             now.Format(time.RFC3339),
		"server_utc_time": now.Format("15:04:05 UTC"),
		"server": map[string]any{
			"description":     config.ProductName,
			"faktory_version": config.Version,
			"uptime":          int(time.Since(s.started).Seconds()),
			"connections":     s.connectionCount(),
			"workers":         s.manager.WorkerCount(),
		},
		"faktory": map[string]any{
			"default_size":    stats.Queues[job.DefaultQueue],
			"total_failures":  stats.Failures,
			"total_processed": stats.Processed,
			"total_enqueued":  enqueued,
			"total_queues":    len(stats.Queues),
			"queues":          stats.Queues,
			"tasks": map[string]any{
				"Scheduled": map[string]uint64{"size": stats.Scheduled},
				"Retries":   map[string]uint64{"size": stats.Retries},
				"Dead":      map[string]uint64{"size": stats.Dead},
				"Busy":      map[string]uint64{"size": stats.Working},
			},
		},
	})
	if err != nil {
		return err
	}

	return protocol.WriteBulk(conn, encoded)
}

func (s *Server) flush(conn net.Conn) error {
	if !s.allowFlush {
		return errors.New("FLUSH is disabled, set ALLOW_FLUSH=true to enable it")
	}

	if err := s.manager.Flush(); err != nil {
		return err
	}

	return protocol.WriteSimple(conn, "OK")
}

func sanitize(err error) string {
	return strings.NewReplacer("\r", " ", "\n", " ").Replace(err.Error())
}
