package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadExpandsEnv(t *testing.T) {
	root := t.TempDir()
	cfgDir := filepath.Join(root, ".kiwi")
	_ = os.MkdirAll(cfgDir, 0755)
	body := `
[search.vector.embedder]
api_key = "${KIWI_TEST_KEY}"
`
	_ = os.WriteFile(filepath.Join(cfgDir, "config.toml"), []byte(body), 0644)
	t.Setenv("KIWI_TEST_KEY", "secret")

	cfg, err := Load(root)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Search.Vector.Embedder.APIKey != "secret" {
		t.Fatalf("expansion failed: %q", cfg.Search.Vector.Embedder.APIKey)
	}
}

func TestLoadExpandsEnvInAuthAndOIDC(t *testing.T) {
	root := t.TempDir()
	cfgDir := filepath.Join(root, ".kiwi")
	_ = os.MkdirAll(cfgDir, 0755)
	body := `
[auth]
type = "apikey"
api_key = "${KIWI_AUTH_KEY}"

[auth.oidc]
issuer = "${KIWI_OIDC_ISSUER}"
client_id = "${KIWI_OIDC_CLIENT}"

[[auth.api_keys]]
key = "${KIWI_TEAM_KEY}"
space = "team"
actor = "team-bot"
`
	_ = os.WriteFile(filepath.Join(cfgDir, "config.toml"), []byte(body), 0644)
	t.Setenv("KIWI_AUTH_KEY", "topsecret")
	t.Setenv("KIWI_OIDC_ISSUER", "https://idp.example/")
	t.Setenv("KIWI_OIDC_CLIENT", "kiwi-app")
	t.Setenv("KIWI_TEAM_KEY", "perspace-secret")

	cfg, err := Load(root)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Auth.APIKey != "topsecret" {
		t.Fatalf("auth.api_key not expanded: %q", cfg.Auth.APIKey)
	}
	if cfg.Auth.OIDC.Issuer != "https://idp.example/" {
		t.Fatalf("auth.oidc.issuer not expanded: %q", cfg.Auth.OIDC.Issuer)
	}
	if cfg.Auth.OIDC.ClientID != "kiwi-app" {
		t.Fatalf("auth.oidc.client_id not expanded: %q", cfg.Auth.OIDC.ClientID)
	}
	if len(cfg.Auth.APIKeys) != 1 || cfg.Auth.APIKeys[0].Key != "perspace-secret" {
		t.Fatalf("per-space key not expanded: %+v", cfg.Auth.APIKeys)
	}
}

func TestVersioningMaxVersionsTOML(t *testing.T) {
	root := t.TempDir()
	cfgDir := filepath.Join(root, ".kiwi")
	_ = os.MkdirAll(cfgDir, 0755)
	body := `
[versioning]
strategy = "cow"
max_versions = 25
`
	_ = os.WriteFile(filepath.Join(cfgDir, "config.toml"), []byte(body), 0644)
	cfg, err := Load(root)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Versioning.MaxVersions != 25 {
		t.Fatalf("want 25, got %d", cfg.Versioning.MaxVersions)
	}
}
