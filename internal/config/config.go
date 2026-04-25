package config

import (
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"

	"github.com/BurntSushi/toml"
)

type Config struct {
	Server     ServerConfig     `toml:"server"`
	Storage    StorageConfig    `toml:"storage"`
	Search     SearchConfig     `toml:"search"`
	Versioning VersioningConfig `toml:"versioning"`
	Auth       AuthConfig       `toml:"auth"`
	Assets     AssetsConfig     `toml:"assets"`
	UI         UIConfig         `toml:"ui"`
	Backup     BackupConfig     `toml:"backup"`
	Janitor    JanitorConfig    `toml:"janitor"`
	Dataview   DataviewConfig   `toml:"dataview"`
	// Spaces enables multi-tenant mode: each entry becomes an
	// independent knowledge base mapped under /api/kiwi/{name}/...
	// When empty, the server runs single-space against Storage.Root.
	Spaces []SpaceConfig `toml:"spaces"`
}

type JanitorConfig struct {
	Interval    string `toml:"interval"`
	StaleDays   int    `toml:"stale_days"`
	StartupScan bool   `toml:"startup_scan"`
}

type DataviewConfig struct {
	MaxScanRows    int    `toml:"max_scan_rows"`
	QueryTimeout   string `toml:"query_timeout"`
	MaxAutoIndexes int    `toml:"max_auto_indexes"`
	ComputedFields bool   `toml:"computed_fields"`
}

// BackupConfig controls automatic git push to a remote repository.
// When Remote is non-empty, the server pushes on a ticker so the
// knowledge base has an off-host copy. Env vars KIWI_BACKUP_REMOTE
// and KIWI_BACKUP_INTERVAL override TOML values for Docker-native config.
type BackupConfig struct {
	Remote   string `toml:"remote"`   // e.g. "git@github.com:user/kb.git"
	Interval string `toml:"interval"` // duration string, default "5m"
	Branch   string `toml:"branch"`   // default: current branch
}

// UIConfig controls frontend behaviour. Toggled via [ui] in config.toml.
type UIConfig struct {
	ThemeLocked bool `toml:"theme_locked"`
}

// AssetsConfig controls binary upload limits and MIME allowlist. Zero values
// trigger the defaults enforced by handlers (10 MB, the common image + PDF
// types) so a missing [assets] section still produces a sane policy.
type AssetsConfig struct {
	MaxFileSize  string   `toml:"max_file_size"` // humanized, e.g. "10MB"
	AllowedTypes []string `toml:"allowed_types"` // MIME allowlist
}

// SpaceConfig is one entry in the multi-space roster.
type SpaceConfig struct {
	Name string `toml:"name"`
	Root string `toml:"root"`
}

type ServerConfig struct {
	Host        string   `toml:"host"`
	Port        int      `toml:"port"`
	CORSOrigins []string `toml:"cors_origins"`
	PublicURL   string   `toml:"public_url"`
}

type StorageConfig struct {
	Root string `toml:"root"`
}

type SearchConfig struct {
	Engine         string       `toml:"engine"` // grep | sqlite
	AsyncIndex     *bool        `toml:"async_index"`      // default true for sqlite
	IndexWindowMs  int          `toml:"index_window_ms"`  // default 200
	IndexBatchMax  int          `toml:"index_batch_max"`  // default 100
	Vector         VectorConfig `toml:"vector"`
}

// VectorConfig turns on semantic search and wires an embedder to a store.
// Both [search.vector.embedder] and [search.vector.store] are required when
// enabled = true.
type VectorConfig struct {
	Enabled  bool                `toml:"enabled"`
	Embedder EmbedderConfig      `toml:"embedder"`
	Store    VectorStoreConfig   `toml:"store"`
	Chunk    VectorChunkConfig   `toml:"chunk"`
}

type EmbedderConfig struct {
	Provider   string            `toml:"provider"`    // openai | ollama | http | cohere | bedrock | vertex
	Model      string            `toml:"model"`
	APIKey     string            `toml:"api_key"`     // ${ENV} expansion supported
	BaseURL    string            `toml:"base_url"`
	URL        string            `toml:"url"`         // provider=http
	Dimensions int               `toml:"dimensions"`
	Headers    map[string]string `toml:"headers"`     // provider=http

	// provider=bedrock
	Region string `toml:"region"`

	// provider=vertex
	Project          string `toml:"project"`           // GCP project id
	Location         string `toml:"location"`          // e.g. "us-central1"
	CredentialsFile  string `toml:"credentials_file"`  // path to service account JSON (optional; falls back to ADC)
}

type VectorStoreConfig struct {
	Provider string `toml:"provider"` // sqlite | qdrant | pinecone | weaviate | pgvector

	// HTTP-based stores (qdrant, pinecone, weaviate) share these.
	URL        string `toml:"url"`
	APIKey     string `toml:"api_key"`
	Collection string `toml:"collection"` // qdrant collection / weaviate class / pinecone index
	Namespace  string `toml:"namespace"`  // pinecone namespace (optional)

	// pgvector — DSN is a standard postgres connection string.
	DSN   string `toml:"dsn"`
	Table string `toml:"table"`
}

type VectorChunkConfig struct {
	Size    int `toml:"size"`
	Overlap int `toml:"overlap"`
}

type VersioningConfig struct {
	Strategy      string `toml:"strategy"`        // git | cow | none
	AsyncCommit   *bool  `toml:"async_commit"`    // default true for git
	BatchWindowMs int    `toml:"batch_window_ms"` // default 200
	BatchMaxSize  int    `toml:"batch_max_size"`  // default 50
	// MaxVersions caps the number of snapshots kept per file when strategy =
	// "cow". Zero means "unbounded" (not recommended — .versions/ grows
	// forever). The spec default is 100; callers that want explicit opt-in
	// can leave it zero and set it in config.toml.
	MaxVersions int `toml:"max_versions"`
}

type AuthConfig struct {
	Type    string       `toml:"type"`    // none | apikey | perspace | oidc
	APIKey  string       `toml:"api_key"` // single global key for type=apikey
	APIKeys []APIKeyEntry `toml:"api_keys"` // per-space keys for type=perspace
	OIDC    OIDCConfig   `toml:"oidc"`
}

type APIKeyEntry struct {
	Key   string `toml:"key"`
	Space string `toml:"space"`
	Actor string `toml:"actor"`
}

type OIDCConfig struct {
	Issuer   string `toml:"issuer"`
	ClientID string `toml:"client_id"`
}

// Load reads .kiwi/config.toml from root. Missing file returns an empty Config.
// String fields of the form ${ENV_VAR} are expanded from the process
// environment — useful for keeping secrets (api_key) out of the repo. The
// expansion walks every exported string field via reflect so adding a new
// secret-bearing field to Config doesn't require remembering to list it
// here; this caught a real bug where Auth.APIKey ignored ${…}.
func Load(root string) (*Config, error) {
	var cfg Config
	path := filepath.Join(root, ".kiwi", "config.toml")
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return nil, err
	}
	expandAllEnv(&cfg)
	applyBackupEnv(&cfg)
	return &cfg, nil
}

// ResolvedPublicURL returns the public URL for building permalinks.
// Priority: explicit public_url > KIWI_PUBLIC_URL env var.
// Returns "" when neither is configured — the localhost fallback is only
// useful for the UI's own routing, not for shareable permalinks.
func (c *Config) ResolvedPublicURL() string {
	if c.Server.PublicURL != "" {
		return strings.TrimRight(c.Server.PublicURL, "/")
	}
	return ""
}

// Permalink returns the full permalink URL for a given file path.
// Each path segment is individually URL-encoded per RFC 3986.
// Returns "" when publicURL is empty.
func Permalink(publicURL, path string) string {
	if publicURL == "" {
		return ""
	}
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for i, s := range segments {
		segments[i] = url.PathEscape(s)
	}
	return publicURL + "/page/" + strings.Join(segments, "/")
}

func applyBackupEnv(cfg *Config) {
	if v := os.Getenv("KIWI_BACKUP_REMOTE"); v != "" {
		cfg.Backup.Remote = v
	}
	if v := os.Getenv("KIWI_BACKUP_INTERVAL"); v != "" {
		cfg.Backup.Interval = v
	}
	if v := os.Getenv("KIWI_PUBLIC_URL"); v != "" {
		cfg.Server.PublicURL = v
	}
}

// expandAllEnv walks every exported string field (at any nesting depth)
// reachable from v and replaces ${VAR} with the environment value. Also
// covers []string, map[string]string, and string slices of structs.
//
// We stop at unexported fields (`reflect` can't set them) and at types
// we don't recognise — there's no reason for a hidden env reference in
// an int, bool, or time.Time. Keeping the set conservative means a
// malformed config can't accidentally rewrite a numeric field.
func expandAllEnv(v interface{}) {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	walkForEnv(rv)
}

func walkForEnv(rv reflect.Value) {
	if !rv.IsValid() {
		return
	}
	switch rv.Kind() {
	case reflect.Struct:
		for i := 0; i < rv.NumField(); i++ {
			// PkgPath != "" → unexported; reflect.Set would panic, and
			// there's no legitimate reason to put a secret in a private
			// field anyway.
			if rv.Type().Field(i).PkgPath != "" {
				continue
			}
			walkForEnv(rv.Field(i))
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < rv.Len(); i++ {
			walkForEnv(rv.Index(i))
		}
	case reflect.Map:
		if rv.Type().Elem().Kind() != reflect.String {
			return
		}
		for _, k := range rv.MapKeys() {
			rv.SetMapIndex(k, reflect.ValueOf(expandEnv(rv.MapIndex(k).String())))
		}
	case reflect.Ptr:
		if !rv.IsNil() {
			walkForEnv(rv.Elem())
		}
	case reflect.String:
		if rv.CanSet() {
			rv.SetString(expandEnv(rv.String()))
		}
	}
}

// envRe matches ${VAR} — used for inline substitution of config strings.
var envRe = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// expandEnv replaces ${VAR} occurrences with os.Getenv("VAR"). Unset variables
// expand to empty string (same as shell behaviour). Values without any ${...}
// pass through untouched.
func expandEnv(s string) string {
	if s == "" || !containsEnvRef(s) {
		return s
	}
	return envRe.ReplaceAllStringFunc(s, func(m string) string {
		name := m[2 : len(m)-1]
		return os.Getenv(name)
	})
}

func containsEnvRef(s string) bool {
	for i := 0; i+1 < len(s); i++ {
		if s[i] == '$' && s[i+1] == '{' {
			return true
		}
	}
	return false
}
