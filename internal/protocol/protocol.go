// Package protocol reads commands and writes RESP replies.
package protocol

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

// MaxLineSize is the largest accepted command, which includes the job payload.
const MaxLineSize = 16 << 20

var ErrLineTooLong = errors.New("command is too long")

func ReadLine(reader *bufio.Reader) ([]byte, error) {
	var line []byte

	for {
		chunk, err := reader.ReadSlice('\n')
		line = append(line, chunk...)

		if len(line) > MaxLineSize {
			return nil, ErrLineTooLong
		}

		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}

		if err != nil {
			return nil, err
		}

		return bytes.TrimSuffix(bytes.TrimSuffix(line, []byte("\n")), []byte("\r")), nil
	}
}

// SplitCommand separates the verb from its arguments.
func SplitCommand(line []byte) (string, []byte) {
	verb, args, _ := bytes.Cut(line, []byte(" "))

	return string(verb), args
}

func WriteSimple(w io.Writer, value string) error {
	_, err := fmt.Fprintf(w, "+%s\r\n", value)

	return err
}

func WriteError(w io.Writer, message string) error {
	_, err := fmt.Fprintf(w, "-ERR %s\r\n", message)

	return err
}

// WriteCoded sends an error with its own code, which clients can detect.
func WriteCoded(w io.Writer, code, message string) error {
	_, err := fmt.Fprintf(w, "-%s %s\r\n", code, message)

	return err
}

func WriteBulk(w io.Writer, value []byte) error {
	if value == nil {
		_, err := io.WriteString(w, "$-1\r\n")

		return err
	}

	_, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(value), value)

	return err
}

// PasswordHash applies SHA-256 to password+salt, then to the digest until the count ends.
func PasswordHash(password, salt string, iterations int) string {
	digest := sha256.Sum256([]byte(password + salt))

	for i := 1; i < iterations; i++ {
		digest = sha256.Sum256(digest[:])
	}

	return hex.EncodeToString(digest[:])
}
