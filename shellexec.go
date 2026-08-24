package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

// Session is worker-level persistent shell state: exported variables and
// the working directory survive across jobs (design decision B).
type Session struct {
	mu  sync.Mutex
	env map[string]string
	dir string
}

func NewSession(dir string) *Session {
	return &Session{env: map[string]string{}, dir: dir}
}

func (s *Session) Env() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mergedEnvLocked()
}

func (s *Session) Dir() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dir
}

func (s *Session) mergedEnvLocked() []string {
	out := os.Environ()
	for k, v := range s.env {
		out = append(out, k+"="+v)
	}
	return out
}

func (s *Session) Export(key, val string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.env[key] = val
}

func (s *Session) Unset(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.env, key)
}

func (s *Session) Chdir(dir string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if filepath.IsAbs(dir) {
		s.dir = filepath.Clean(dir)
	} else {
		s.dir = filepath.Clean(filepath.Join(s.dir, dir))
	}
	return s.dir
}

// ---------- word expansion ----------

func isNameByte(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

func expandVars(s string, lookup func(string) string) (string, *lexError) {
	var b strings.Builder
	i := 0
	n := len(s)
	for i < n {
		c := s[i]
		if c != '$' {
			b.WriteByte(c)
			i++
			continue
		}
		if i+1 >= n {
			b.WriteByte('$')
			break
		}
		next := s[i+1]
		switch {
		case next == '(':
			return "", &lexError{msg: errSub, pos: i}
		case next == '{':
			j := i + 2
			for j < n && s[j] != '}' && isNameByte(s[j]) {
				j++
			}
			if j >= n || s[j] != '}' {
				return "", &lexError{msg: "unterminated ${", pos: i}
			}
			name := s[i+2 : j]
			if name == "" {
				return "", &lexError{msg: errSpec, pos: i}
			}
			b.WriteString(lookup(name))
			i = j + 1
		case isNameByte(next):
			j := i + 1
			for j < n && isNameByte(s[j]) {
				j++
			}
			b.WriteString(lookup(s[i+1 : j]))
			i = j
		default:
			return "", &lexError{msg: errSpec, pos: i}
		}
	}
	return b.String(), nil
}

// expandWord turns parsed word parts into final argv words, applying
// variable expansion (bare + double-quoted parts) and glob expansion
// (bare parts only).
func expandWord(parts []wordPart, lookup func(string) string, cwd string) ([]string, *lexError) {
	var raw strings.Builder
	hasBareGlob := false
	for _, p := range parts {
		switch p.class {
		case classSingle:
			raw.WriteString(p.val)
		case classDouble:
			exp, lerr := expandVars(p.val, lookup)
			if lerr != nil {
				return nil, lerr
			}
			raw.WriteString(exp)
		default:
			exp, lerr := expandVars(p.val, lookup)
			if lerr != nil {
				return nil, lerr
			}
			raw.WriteString(exp)
			if strings.ContainsAny(p.val, "*?[") {
				hasBareGlob = true
			}
		}
	}
	word := raw.String()
	if !hasBareGlob {
		return []string{word}, nil
	}
	matches, err := filepath.Glob(filepath.Join(cwd, word))
	if err != nil || len(matches) == 0 {
		return []string{word}, nil
	}
	rel := make([]string, len(matches))
	for i, m := range matches {
		r, err2 := filepath.Rel(cwd, m)
		if err2 != nil {
			rel[i] = m
		} else {
			rel[i] = r
		}
	}
	return rel, nil
}

// ---------- builtins ----------

func runBuiltin(argv []string, sess *Session) (bool, int, string) {
	switch argv[0] {
	case "cd":
		if len(argv) > 2 {
			return true, 2, "cd: too many arguments"
		}
		target := ""
		if len(argv) == 2 {
			target = argv[1]
		} else {
			if h := os.Getenv("HOME"); h != "" {
				target = h
			} else {
				target = "/"
			}
		}
		newDir := sess.Chdir(target)
		if fi, err := os.Stat(newDir); err != nil || !fi.IsDir() {
			return true, 1, "cd: " + target + ": not a directory"
		}
		return true, 0, ""
	case "export":
		if len(argv) == 1 {
			return true, 0, ""
		}
		for _, a := range argv[1:] {
			k, v, ok := strings.Cut(a, "=")
			if !ok || k == "" {
				return true, 1, "export: expected KEY=VALUE"
			}
			sess.Export(k, v)
		}
		return true, 0, ""
	case "unset":
		if len(argv) == 1 {
			return true, 0, ""
		}
		for _, k := range argv[1:] {
			sess.Unset(k)
		}
		return true, 0, ""
	}
	return false, 0, ""
}

// ---------- process execution ----------

func newExecCmd(argv []string, sess *Session) *exec.Cmd {
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Dir = sess.Dir()
	cmd.Env = sess.Env()
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return cmd
}
