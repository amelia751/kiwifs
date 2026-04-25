package rbac

import (
	"bytes"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	VisibilityPrivate  = "private"
	VisibilityInternal = "internal"
	VisibilityPublic   = "public"
	VisibilityPassword = "password"
)

const (
	RoleAdmin  = "admin"
	RoleEditor = "editor"
	RoleViewer = "viewer"
	RolePublic = "public"
)

type Permission struct {
	CanRead   bool
	CanWrite  bool
	CanDelete bool
	CanAdmin  bool
}

// RolePermissions returns the permissions for a given role.
func RolePermissions(role string) Permission {
	switch role {
	case RoleAdmin:
		return Permission{CanRead: true, CanWrite: true, CanDelete: true, CanAdmin: true}
	case RoleEditor:
		return Permission{CanRead: true, CanWrite: true}
	case RoleViewer:
		return Permission{CanRead: true}
	case RolePublic:
		return Permission{CanRead: true}
	default:
		return Permission{}
	}
}

// CanAccess checks if a role can access a page with the given visibility.
func CanAccess(role, visibility string) bool {
	if role == RoleAdmin {
		return true
	}
	switch visibility {
	case VisibilityPublic:
		return true
	case VisibilityInternal:
		return role == RoleEditor || role == RoleViewer
	case VisibilityPrivate:
		return role == RoleEditor
	case VisibilityPassword:
		// Password-protected pages are accessible to anyone who supplies the
		// correct page password; the password check itself happens elsewhere.
		return true
	default:
		// Unrecognised visibility treated as internal.
		return role == RoleEditor || role == RoleViewer
	}
}

// PageVisibility extracts the visibility field from YAML frontmatter.
// Returns VisibilityInternal when the field is absent or unparseable.
func PageVisibility(content []byte) string {
	fm, _, err := splitFrontmatter(content)
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

// PageACL holds the per-user overrides embedded in frontmatter:
//
//	---
//	visibility: private
//	owner: alice@acme
//	readers: [bob@acme, carol@acme]
//	editors: [dave@acme]
//	---
//
// These live on top of space-level roles — if a user isn't in the
// space at all they still can't see the page, but within a space the
// ACL is the finer gate. An empty struct means "fall back to the
// space-level role check".
type PageACL struct {
	Owner   string   `yaml:"owner"`
	Editors []string `yaml:"editors"`
	Readers []string `yaml:"readers"`
	// Teams are space-level group names. Parsed but not yet consulted
	// by EvaluatePageAccess; the field is here so the frontmatter
	// surface stays stable while the team directory is wired up.
	Teams []string `yaml:"teams"`
}

// ParsePageACL pulls {owner, readers, editors, teams} out of a file's
// frontmatter. A file without any of those keys returns a zero PageACL,
// not an error — callers treat a zero ACL as "no per-page overrides".
func ParsePageACL(content []byte) (PageACL, error) {
	fm, _, err := splitFrontmatter(content)
	if err != nil || len(fm) == 0 {
		return PageACL{}, err
	}
	var acl PageACL
	if err := yaml.Unmarshal(fm, &acl); err != nil {
		return PageACL{}, err
	}
	return acl, nil
}

// AccessDecision is what EvaluatePageAccess returns so callers can
// build useful error messages instead of an opaque boolean — a 403
// "you are not on the readers list" is much easier to debug than a
// generic "forbidden".
type AccessDecision struct {
	Allowed bool
	Reason  string
}

// EvaluatePageAccess combines per-page ACL + visibility + space-level
// role into a single access decision for a (user, action) pair. Action
// is one of "read", "write", "delete".
//
// Order of precedence (high → low):
//  1. Owner has full access regardless of role or visibility.
//  2. Explicit editors list grants read+write.
//  3. Explicit readers list grants read only.
//  4. Otherwise fall back to visibility + space role.
//
// Anonymous callers (user == "") only pass step 4, and only for
// VisibilityPublic pages.
func EvaluatePageAccess(acl PageACL, visibility, user, role, action string) AccessDecision {
	switch action {
	case "read", "write", "delete":
	default:
		return AccessDecision{Allowed: false, Reason: "unknown action"}
	}

	if role == RoleAdmin {
		return AccessDecision{Allowed: true, Reason: "admin"}
	}

	if user != "" {
		if acl.Owner != "" && strings.EqualFold(acl.Owner, user) {
			return AccessDecision{Allowed: true, Reason: "owner"}
		}
		if containsCI(acl.Editors, user) {
			return AccessDecision{Allowed: true, Reason: "page editor"}
		}
		if containsCI(acl.Readers, user) {
			if action == "read" {
				return AccessDecision{Allowed: true, Reason: "page reader"}
			}
			return AccessDecision{Allowed: false, Reason: "readers cannot " + action}
		}
	}

	// Fall back to visibility + role.
	if action == "read" {
		if CanAccess(role, visibility) {
			return AccessDecision{Allowed: true, Reason: "space role"}
		}
		return AccessDecision{Allowed: false, Reason: "space role lacks read on " + visibility + " page"}
	}

	perms := RolePermissions(role)
	switch action {
	case "write":
		if perms.CanWrite {
			return AccessDecision{Allowed: true, Reason: "space role"}
		}
	case "delete":
		if perms.CanDelete {
			return AccessDecision{Allowed: true, Reason: "space role"}
		}
	}
	return AccessDecision{Allowed: false, Reason: "space role lacks " + action}
}

func containsCI(list []string, needle string) bool {
	for _, v := range list {
		if strings.EqualFold(v, needle) {
			return true
		}
	}
	return false
}

// splitFrontmatter mirrors pipeline.splitFrontmatter: returns the YAML block
// (without delimiters) and the remaining body.
func splitFrontmatter(content []byte) (fm, body []byte, err error) {
	delim := []byte("---")

	line, rest, ok := bytes.Cut(content, []byte("\n"))
	if !ok {
		return nil, content, nil
	}
	if !bytes.Equal(bytes.TrimRight(line, "\r"), delim) {
		return nil, content, nil
	}
	scanner := rest
	pos := 0
	for {
		nl := bytes.IndexByte(scanner, '\n')
		var current []byte
		if nl < 0 {
			current = scanner
		} else {
			current = scanner[:nl]
		}
		if bytes.Equal(bytes.TrimRight(current, "\r"), delim) {
			fm = rest[:pos]
			if nl < 0 {
				body = nil
			} else {
				body = scanner[nl+1:]
			}
			return fm, body, nil
		}
		if nl < 0 {
			return nil, nil, nil
		}
		pos += nl + 1
		scanner = scanner[nl+1:]
	}
}
