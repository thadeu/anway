// Package testclient is a minimal protocol client for tests and load tools.
package testclient

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"jobserver/internal/protocol"
)

const handshakeTimeout = 10 * time.Second

type Client struct {
	conn   net.Conn
	reader *bufio.Reader
}

// Dial opens a connection. A blank wid creates a producer.
func Dial(addr, wid, password string) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}

	conn.SetDeadline(time.Now().Add(handshakeTimeout))

	client := &Client{conn: conn, reader: bufio.NewReader(conn)}

	greeting, err := client.reply()
	if err != nil {
		conn.Close()

		return nil, err
	}

	var hi struct {
		Salt       string `json:"s"`
		Iterations int    `json:"i"`
	}

	if err := json.Unmarshal([]byte(strings.TrimPrefix(greeting, "HI ")), &hi); err != nil {
		conn.Close()

		return nil, err
	}

	hello := map[string]any{"v": 2}

	if wid != "" {
		hello["wid"] = wid
		hello["hostname"] = "test"
		hello["pid"] = 1
		hello["labels"] = []string{"test"}
	}

	if hi.Salt != "" {
		hello["pwdhash"] = protocol.PasswordHash(password, hi.Salt, hi.Iterations)
	}

	if _, err := client.Command("HELLO", hello); err != nil {
		conn.Close()

		return nil, err
	}

	conn.SetDeadline(time.Time{})

	return client, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// Command sends a verb with an optional JSON argument, and returns the reply.
func (c *Client) Command(verb string, argument any) (string, error) {
	line := verb

	if text, ok := argument.(string); ok {
		line += " " + text
	} else if argument != nil {
		encoded, err := json.Marshal(argument)
		if err != nil {
			return "", err
		}

		line += " " + string(encoded)
	}

	if _, err := io.WriteString(c.conn, line+"\r\n"); err != nil {
		return "", err
	}

	return c.reply()
}

func (c *Client) reply() (string, error) {
	line, err := protocol.ReadLine(c.reader)
	if err != nil {
		return "", err
	}

	if len(line) == 0 {
		return "", errors.New("empty reply")
	}

	text := string(line[1:])

	switch line[0] {
	case '+':
		return text, nil
	case '-':
		return "", errors.New(text)
	case '$':
		return c.bulk(text)
	default:
		return "", fmt.Errorf("unexpected reply: %q", line)
	}
}

func (c *Client) bulk(size string) (string, error) {
	length, err := strconv.Atoi(size)
	if err != nil {
		return "", err
	}

	if length < 0 {
		return "", nil
	}

	body := make([]byte, length+2)

	if _, err := io.ReadFull(c.reader, body); err != nil {
		return "", err
	}

	return string(body[:length]), nil
}
