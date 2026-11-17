package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"

	"github.com/thadeu/anway/internal/cron"
	"github.com/thadeu/anway/internal/protocol"
)

var errNoScheduler = errors.New("schedules are not available")

// schedule runs SCHEDULE: it replaces all cron entries of a client tag. An
// unknown key is an error, as in a conf.d file, so a typing error does not
// pass in silence.
func (s *Server) schedule(conn net.Conn, args []byte) error {
	if s.scheduler == nil {
		return errNoScheduler
	}

	var request cron.ScheduleRequest

	decoder := json.NewDecoder(bytes.NewReader(args))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&request); err != nil {
		return fmt.Errorf("invalid SCHEDULE: %w", err)
	}

	if err := s.scheduler.Schedule(request); err != nil {
		return err
	}

	return protocol.WriteSimple(conn, "OK")
}

// unschedule runs UNSCHEDULE: it removes all cron entries of a client tag. A
// missing tag is not an error.
func (s *Server) unschedule(conn net.Conn, args []byte) error {
	if s.scheduler == nil {
		return errNoScheduler
	}

	var request struct {
		Tag string `json:"tag"`
	}

	if err := json.Unmarshal(args, &request); err != nil {
		return fmt.Errorf("invalid UNSCHEDULE: %w", err)
	}

	if err := s.scheduler.Unschedule(request.Tag); err != nil {
		return err
	}

	return protocol.WriteSimple(conn, "OK")
}
