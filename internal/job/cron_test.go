package job

import (
	"strings"
	"testing"
)

func TestCronJIDNamesItsEntry(t *testing.T) {
	for _, manual := range []bool{false, true} {
		jid, err := CronJID("0123456789ab", manual)
		if err != nil {
			t.Fatal(err)
		}

		if manual != strings.HasPrefix(jid, "cron-manual-") {
			t.Fatalf("unexpected jid %q", jid)
		}

		entry, gotManual, ok := CronEntry(jid)

		if !ok || entry != "0123456789ab" || gotManual != manual {
			t.Fatalf("CronEntry(%q) = %q, %v, %v", jid, entry, gotManual, ok)
		}
	}
}

func TestCronEntryRefusesOtherJIDs(t *testing.T) {
	cases := []string{
		"web-123",
		"cron-0123456789abcdef01234567",
		"cron-0123456789AB-ff",
		"cron-0123-ff",
		"cron-manual-",
		"",
	}

	for _, jid := range cases {
		if _, _, ok := CronEntry(jid); ok {
			t.Fatalf("%q must not name an entry", jid)
		}
	}
}
