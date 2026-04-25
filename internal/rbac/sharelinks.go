package rbac

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/kiwifs/kiwifs/internal/markdown"
	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"
)

const (
	VisibilityPrivate  = "private"
	VisibilityInternal = "internal"
	VisibilityPublic   = "public"
	VisibilityPassword = "password"
)

// PageVisibility extracts the visibility field from YAML frontmatter.
// Returns VisibilityInternal when the field is absent or unparseable.
func PageVisibility(content []byte) string {
	fm, _, err := markdown.SplitFrontmatter(content)
	if err != nil || len(fm) == 0 {
		return VisibilityInternal
	}
	var meta struct {
		Visibility string `yaml:"visibility"`
	}
	if err := yaml.Unmarshal(fm, &meta); err != nil {
		return VisibilityInternal
	}
	switch meta.Visibility {
	case VisibilityPrivate, VisibilityInternal, VisibilityPublic, VisibilityPassword:
		return meta.Visibility
	default:
		return VisibilityInternal
	}
}

// ShareLink is the on-disk + wire form of a public share entry.
// PasswordHash is the sha256 hex of the (salt + password) secret, never the
// raw password — older entries with a bare Password field are upgraded on load.
type ShareLink struct {
	ID           string    `json:"id"`
	Path         string    `json:"path"`
	Token        string    `json:"token"`
	ExpiresAt    time.Time `json:"expiresAt,omitempty"`
	PasswordSalt string    `json:"passwordSalt,omitempty"`
	PasswordHash string    `json:"passwordHash,omitempty"`
	// Password is accepted from API clients for convenience but is never
	// serialised back out — the server hashes it immediately.
	Password  string    `json:"password,omitempty"`
	CreatedBy string    `json:"createdBy"`
	CreatedAt time.Time `json:"createdAt"`
	ViewCount int       `json:"viewCount"`
}

// HasPassword reports whether the link requires a password to be resolved.
func (l *ShareLink) HasPassword() bool {
	return l != nil && l.PasswordHash != ""
}

// ErrInvalidPassword is returned by Resolve when the caller supplied a wrong
// or missing password for a password-protected link.
var ErrInvalidPassword = errors.New("sharelinks: invalid password")

// ShareStore manages share link persistence in .kiwi/state/sharelinks.json.
type ShareStore struct {
	path string
	mu   sync.RWMutex
	links map[string]*ShareLink // token → link
}

// NewShareStore loads existing share links from disk (or starts empty).
func NewShareStore(root string) (*ShareStore, error) {
	dir := filepath.Join(root, ".kiwi", "state")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("sharelinks: mkdir: %w", err)
	}
	s := &ShareStore{
		path:  filepath.Join(dir, "sharelinks.json"),
		links: make(map[string]*ShareLink),
	}
	if err := s.load(); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("sharelinks: load: %w", err)
	}
	return s, nil
}

// Create generates a new share link for path. expiresIn of 0 means no expiry.
// When password is non-empty the link is stored as a salted sha256 hash.
func (s *ShareStore) Create(path, createdBy string, expiresIn time.Duration, password string) (*ShareLink, error) {
	token, err := randomHex(32)
	if err != nil {
		return nil, fmt.Errorf("sharelinks: generate token: %w", err)
	}
	id, err := randomHex(8)
	if err != nil {
		return nil, fmt.Errorf("sharelinks: generate id: %w", err)
	}

	now := time.Now().UTC()
	link := &ShareLink{
		ID:        id,
		Path:      path,
		Token:     token,
		CreatedBy: createdBy,
		CreatedAt: now,
	}
	if expiresIn > 0 {
		link.ExpiresAt = now.Add(expiresIn)
	}
	if password != "" {
		hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		if err != nil {
			return nil, fmt.Errorf("sharelinks: hash password: %w", err)
		}
		link.PasswordHash = string(hash)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.links[token] = link
	if err := s.save(); err != nil {
		delete(s.links, token)
		return nil, err
	}
	return sanitize(link), nil
}

// Resolve looks up a share link by token, applying expiry and password checks.
// Returns (nil, nil) when not found or expired. Returns ErrInvalidPassword
// when the link is password-protected and the caller's password does not match.
func (s *ShareStore) Resolve(token, password string) (*ShareLink, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	link, ok := s.links[token]
	if !ok {
		return nil, nil
	}
	if !link.ExpiresAt.IsZero() && time.Now().After(link.ExpiresAt) {
		delete(s.links, token)
		_ = s.save()
		return nil, nil
	}
	if link.PasswordHash != "" {
		if err := bcrypt.CompareHashAndPassword([]byte(link.PasswordHash), []byte(password)); err != nil {
			return nil, ErrInvalidPassword
		}
	}
	link.ViewCount++
	_ = s.save()
	return sanitize(link), nil
}

// Revoke removes a share link by id.
func (s *ShareStore) Revoke(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for token, link := range s.links {
		if link.ID == id {
			delete(s.links, token)
			return s.save()
		}
	}
	return fmt.Errorf("share link %q not found", id)
}

// ListForPath returns all active share links for the given path (with secrets redacted).
func (s *ShareStore) ListForPath(path string) ([]*ShareLink, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now()
	var out []*ShareLink
	for _, link := range s.links {
		if link.Path != path {
			continue
		}
		if !link.ExpiresAt.IsZero() && now.After(link.ExpiresAt) {
			continue
		}
		out = append(out, sanitize(link))
	}
	return out, nil
}

func (s *ShareStore) load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return err
	}
	var all []*ShareLink
	if err := json.Unmarshal(data, &all); err != nil {
		return err
	}
	s.links = make(map[string]*ShareLink, len(all))
	upgraded := false
	for _, l := range all {
		// Migrate older entries that stored the password in plaintext.
		if l.Password != "" && l.PasswordHash == "" {
			hash, err := bcrypt.GenerateFromPassword([]byte(l.Password), bcrypt.DefaultCost)
			if err != nil {
				return fmt.Errorf("sharelinks: upgrade hash: %w", err)
			}
			l.PasswordHash = string(hash)
			l.PasswordSalt = ""
			l.Password = ""
			upgraded = true
		}
		s.links[l.Token] = l
	}
	if upgraded {
		_ = s.save()
	}
	return nil
}

// save writes the current state to disk. Caller must hold s.mu.
func (s *ShareStore) save() error {
	all := make([]*ShareLink, 0, len(s.links))
	for _, l := range s.links {
		all = append(all, l)
	}
	data, err := json.MarshalIndent(all, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, data, 0o600)
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// sanitize returns a shallow copy with secrets removed for outbound responses.
func sanitize(l *ShareLink) *ShareLink {
	if l == nil {
		return nil
	}
	copy := *l
	copy.Password = ""
	copy.PasswordHash = ""
	copy.PasswordSalt = ""
	return &copy
}
