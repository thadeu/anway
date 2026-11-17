package job

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
)

// A job that the scheduler pushes has a jid that names its cron entry, so
// the store can note each run of the entry without an index:
//
//	cron-<entry>-<random>          a run on the schedule
//	cron-manual-<entry>-<random>   a run that an operator asked for
//
// entry is the id of the entry, 12 hex digits. The jid of a server before
// 0.7.0, cron-<random>, names no entry.
const (
	CronIDLength = 12
	cronPrefix   = "cron-"
	manualPrefix = "manual-"
)

// CronJID makes a new jid for a run of a cron entry.
func CronJID(entry string, manual bool) (string, error) {
	random := make([]byte, 8)

	if _, err := rand.Read(random); err != nil {
		return "", err
	}

	prefix := cronPrefix

	if manual {
		prefix += manualPrefix
	}

	return prefix + entry + "-" + hex.EncodeToString(random), nil
}

// CronEntry returns the cron entry that a jid names. ok is false for a jid
// that is not a run of a cron entry.
func CronEntry(jid string) (entry string, manual bool, ok bool) {
	rest, found := strings.CutPrefix(jid, cronPrefix)
	if !found {
		return "", false, false
	}

	rest, manual = strings.CutPrefix(rest, manualPrefix)

	entry, _, found = strings.Cut(rest, "-")
	if !found || !ValidCronID(entry) {
		return "", false, false
	}

	return entry, manual, true
}

// ValidCronID is true for 12 lower case hex digits.
func ValidCronID(id string) bool {
	if len(id) != CronIDLength {
		return false
	}

	for _, char := range id {
		if !strings.ContainsRune("0123456789abcdef", char) {
			return false
		}
	}

	return true
}
