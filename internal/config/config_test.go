package config

import "testing"

func TestValidate(t *testing.T) {
	cases := []struct {
		name  string
		cfg   Config
		valid bool
	}{
		{"loopback without password", Config{BindAddr: "127.0.0.1:7419"}, true},
		{"localhost without password", Config{BindAddr: "localhost:7419"}, true},
		{"public without password", Config{BindAddr: "0.0.0.0:7419"}, false},
		{"blank host without password", Config{BindAddr: ":7419"}, false},
		{"public with password", Config{BindAddr: "0.0.0.0:7419", Password: "x"}, true},
		{"certificate without key", Config{BindAddr: "127.0.0.1:7419", TLSCertFile: "a"}, false},
	}

	for _, current := range cases {
		if err := current.cfg.validate(); (err == nil) != current.valid {
			t.Fatalf("%s: unexpected result: %v", current.name, err)
		}
	}
}

func TestFlagsOverrideEnv(t *testing.T) {
	t.Setenv("BIND_ADDR", "127.0.0.1:1")
	t.Setenv("DATA_DIR", "/env")

	cfg, err := Load([]string{"--bind", "127.0.0.1:2"})
	if err != nil {
		t.Fatal(err)
	}

	if cfg.BindAddr != "127.0.0.1:2" || cfg.DataDir != "/env" || cfg.BackupDir != "/env/backups" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestFlagsAreValidated(t *testing.T) {
	if _, err := Load([]string{"--bind", "0.0.0.0:1"}); err == nil {
		t.Fatal("a public address needs a password")
	}

	if _, err := Load([]string{"extra"}); err == nil {
		t.Fatal("an extra argument must fail")
	}
}
