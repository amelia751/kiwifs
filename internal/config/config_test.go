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

func TestPublicURLFromTOML(t *testing.T) {
	root := t.TempDir()
	cfgDir := filepath.Join(root, ".kiwi")
	_ = os.MkdirAll(cfgDir, 0755)
	body := `
[server]
public_url = "https://wiki.mycompany.com"
host = "0.0.0.0"
port = 3333
`
	_ = os.WriteFile(filepath.Join(cfgDir, "config.toml"), []byte(body), 0644)
	cfg, err := Load(root)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := cfg.ResolvedPublicURL(); got != "https://wiki.mycompany.com" {
		t.Fatalf("want explicit public_url, got %q", got)
	}
}

func TestPublicURLFromEnv(t *testing.T) {
	root := t.TempDir()
	cfgDir := filepath.Join(root, ".kiwi")
	_ = os.MkdirAll(cfgDir, 0755)
	body := `
[server]
host = "0.0.0.0"
port = 3333
`
	_ = os.WriteFile(filepath.Join(cfgDir, "config.toml"), []byte(body), 0644)
	t.Setenv("KIWI_PUBLIC_URL", "https://env.example.com/")
	cfg, err := Load(root)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := cfg.ResolvedPublicURL(); got != "https://env.example.com" {
		t.Fatalf("want env override (trailing slash trimmed), got %q", got)
	}
}

func TestPublicURLDefaultsToEmpty(t *testing.T) {
	root := t.TempDir()
	cfgDir := filepath.Join(root, ".kiwi")
	_ = os.MkdirAll(cfgDir, 0755)
	body := `
[server]
host = "10.0.0.1"
port = 8080
`
	_ = os.WriteFile(filepath.Join(cfgDir, "config.toml"), []byte(body), 0644)
	cfg, err := Load(root)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := cfg.ResolvedPublicURL(); got != "" {
		t.Fatalf("want empty when no public_url configured, got %q", got)
	}
}

func TestListenURLDefaultsToHostPort(t *testing.T) {
	root := t.TempDir()
	cfgDir := filepath.Join(root, ".kiwi")
	_ = os.MkdirAll(cfgDir, 0755)
	body := `
[server]
host = "10.0.0.1"
port = 8080
`
	_ = os.WriteFile(filepath.Join(cfgDir, "config.toml"), []byte(body), 0644)
	cfg, err := Load(root)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := cfg.ResolvedListenURL(); got != "http://10.0.0.1:8080" {
		t.Fatalf("want host:port fallback, got %q", got)
	}
}

func TestPermalink(t *testing.T) {
	cases := []struct {
		publicURL, path, want string
	}{
		{"https://wiki.co", "concepts/auth.md", "https://wiki.co/page/concepts/auth.md"},
		{"https://wiki.co", "/concepts/auth.md", "https://wiki.co/page/concepts/auth.md"},
		{"", "concepts/auth.md", ""},
		{"https://wiki.co", "my notes/auth flow.md", "https://wiki.co/page/my%20notes/auth%20flow.md"},
		{"https://wiki.co", "日本語/ノート.md", "https://wiki.co/page/%E6%97%A5%E6%9C%AC%E8%AA%9E/%E3%83%8E%E3%83%BC%E3%83%88.md"},
		{"https://wiki.co", "file#2.md", "https://wiki.co/page/file%232.md"},
		{"https://wiki.co", "100%.md", "https://wiki.co/page/100%25.md"},
	}
	for _, tc := range cases {
		got := Permalink(tc.publicURL, tc.path)
		if got != tc.want {
			t.Errorf("Permalink(%q, %q) = %q, want %q", tc.publicURL, tc.path, got, tc.want)
		}
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
