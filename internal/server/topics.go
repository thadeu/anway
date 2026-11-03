package server

import (
	"encoding/json"
	"fmt"
	"net"

	"github.com/thadeu/anway/internal/manager"
	"github.com/thadeu/anway/internal/protocol"
)

// publish runs PUBLISH. The reply is JSON with the event id, the number of
// jobs that it made, and duplicate.
func (s *Server) publish(conn net.Conn, args []byte) error {
	var request manager.PublishRequest

	if err := json.Unmarshal(args, &request); err != nil {
		return fmt.Errorf("invalid PUBLISH: %w", err)
	}

	published, err := s.manager.Publish(request)
	if err != nil {
		return err
	}

	encoded, err := json.Marshal(published)
	if err != nil {
		return err
	}

	return protocol.WriteBulk(conn, encoded)
}

// subscribe runs SUBSCRIBE or UNSUBSCRIBE. Both are idempotent.
func (s *Server) subscribe(conn net.Conn, args []byte, apply func(manager.SubscribeRequest) error) error {
	var request manager.SubscribeRequest

	if err := json.Unmarshal(args, &request); err != nil {
		return fmt.Errorf("invalid subscription: %w", err)
	}

	if err := apply(request); err != nil {
		return err
	}

	return protocol.WriteSimple(conn, "OK")
}
