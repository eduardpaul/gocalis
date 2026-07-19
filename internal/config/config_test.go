package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTempConfig(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return p
}

const telegramCreds = `
telegram:
  api_id: 123
  api_hash: "abc"
`

func TestLoadConfigTelegramValid(t *testing.T) {
	p := writeTempConfig(t, telegramCreds+`
nodes:
  - node_id: tg_group
    type: telegram
    telegram:
      target_type: group
      target: "@fam"
      autowake: true
  - node_id: tg_contact
    type: telegram
    telegram:
      target_type: contact
      target: "@alice"
`)
	cfg, err := LoadConfig(p)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if !cfg.Telegram.Enabled() {
		t.Fatal("expected telegram account enabled")
	}
	if got := cfg.Nodes[0].Telegram.GetReadyTimeoutSeconds(); got != 60 {
		t.Fatalf("default ready timeout = %v, want 60", got)
	}
	if !cfg.Nodes[0].EchoCancellationEnabled() {
		t.Fatal("telegram node should default to echo cancellation enabled")
	}
}

func TestLoadConfigAutowakeContactRejected(t *testing.T) {
	p := writeTempConfig(t, telegramCreds+`
nodes:
  - node_id: tg_contact
    type: telegram
    telegram:
      target_type: contact
      target: "@alice"
      autowake: true
`)
	if _, err := LoadConfig(p); err == nil {
		t.Fatal("expected error: autowake is only valid for group target_type")
	}
}

func TestLoadConfigTelegramRequiresTargetAndCreds(t *testing.T) {
	// Missing target.
	p := writeTempConfig(t, telegramCreds+`
nodes:
  - node_id: tg
    type: telegram
    telegram:
      target_type: group
`)
	if _, err := LoadConfig(p); err == nil {
		t.Fatal("expected error for missing telegram.target")
	}

	// Telegram node but no global account configured.
	p2 := writeTempConfig(t, `
nodes:
  - node_id: tg
    type: telegram
    telegram:
      target_type: group
      target: "@fam"
`)
	if _, err := LoadConfig(p2); err == nil {
		t.Fatal("expected error when global telegram account is not configured")
	}
}

func TestLoadConfigInvalidTargetType(t *testing.T) {
	p := writeTempConfig(t, telegramCreds+`
nodes:
  - node_id: tg
    type: telegram
    telegram:
      target_type: channel
      target: "@fam"
`)
	if _, err := LoadConfig(p); err == nil {
		t.Fatal("expected error for invalid target_type")
	}
}
