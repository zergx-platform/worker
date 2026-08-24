package main

import (
	"fmt"
	"strings"
)

type tokKind int

const (
	tokWord tokKind = iota
	tokSep
	tokAnd
	tokOr
	tokEOL
)

type token struct {
	kind  tokKind
	parts []wordPart
	pos   int
}

// quoteClass tracks how a fragment was quoted in the source line.
type quoteClass int

const (
	classBare   quoteClass = iota // variable expansion + glob
	classDouble                   // variable expansion, no glob
	classSingle                   // fully literal
)

type wordPart struct {
	val   string
	class quoteClass
}

type lexError struct {
	msg string
	pos int
}

func (e *lexError) Error() string { return fmt.Sprintf("%s (at position %d)", e.msg, e.pos) }

const (
	errPipe = "pipes are not supported: run the command as-is and inspect full output with job_output (offset/limit/grep)"
	errBg   = "background '&' is not supported: jobs exceeding the sync wait are backgrounded automatically; poll with job_list/job_output"
	errRed  = "output/input redirection is not supported: stdout/stderr are captured automatically, read them via job_output"
	errSub  = "command substitution ($() or backticks) is not supported: get the value first (read tool / job_output), then run the command"
	errGrp  = "subshells/grouping are not supported: issue one command at a time, joined by && || or ;"
	errSpec = "special shell parameters ($? $@ $$ $! $0 etc.) are not supported"
)

var controlWords = map[string]bool{
	"if": true, "then": true, "elif": true, "else": true, "fi": true,
	"for": true, "while": true, "until": true, "do": true, "done": true,
	"case": true, "esac": true, "function": true, "select": true, "time": true,
}

func tokenize(src string) ([]token, error) {
	var toks []token
	i := 0
	n := len(src)
	for i < n {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\r':
			i++
		case c == '\n' || c == ';':
			toks = append(toks, token{kind: tokSep, pos: i})
			i++
		case c == '&':
			if i+1 < n && src[i+1] == '&' {
				toks = append(toks, token{kind: tokAnd, pos: i})
				i += 2
			} else {
				return nil, &lexError{msg: errBg, pos: i}
			}
		case c == '|':
			if i+1 < n && src[i+1] == '|' {
				toks = append(toks, token{kind: tokOr, pos: i})
				i += 2
			} else {
				return nil, &lexError{msg: errPipe, pos: i}
			}
		case c == '>' || c == '<':
			return nil, &lexError{msg: errRed, pos: i}
		case c == '(' || c == ')':
			return nil, &lexError{msg: errGrp, pos: i}
		case c == '{' || c == '}':
			return nil, &lexError{msg: errGrp, pos: i}
		case c == '`':
			return nil, &lexError{msg: errSub, pos: i}
		default:
			tok, next, err := readWord(src, i)
			if err != nil {
				return nil, err
			}
			toks = append(toks, tok)
			i = next
		}
	}
	toks = append(toks, token{kind: tokEOL, pos: n})
	return toks, nil
}

var wordDelims = map[byte]bool{
	' ': true, '\t': true, '\r': true, '\n': true,
	';': true, '&': true, '|': true, '(': true, ')': true,
	'>': true, '<': true, '`': true, '{': true, '}': true,
}

func readWord(src string, i int) (token, int, error) {
	var parts []wordPart
	var b strings.Builder
	flush := func(class quoteClass) {
		if b.Len() > 0 {
			parts = append(parts, wordPart{val: b.String(), class: class})
			b.Reset()
		}
	}
	n := len(src)
	for i < n {
		c := src[i]
		if wordDelims[c] && c != '{' && c != '}' {
			break
		}
		switch c {
		case '\'':
			j := i + 1
			for j < n && src[j] != '\'' {
				j++
			}
			if j >= n {
				return token{}, 0, &lexError{msg: "unterminated single quote", pos: i}
			}
			flush(classBare)
			parts = append(parts, wordPart{val: src[i+1 : j], class: classSingle})
			i = j + 1
		case '"':
			j := i + 1
			var q strings.Builder
			for j < n && src[j] != '"' {
				if src[j] == '\\' && j+1 < n && (src[j+1] == '"' || src[j+1] == '\\' || src[j+1] == '$') {
					q.WriteByte(src[j+1])
					j += 2
					continue
				}
				q.WriteByte(src[j])
				j++
			}
			if j >= n {
				return token{}, 0, &lexError{msg: "unterminated double quote", pos: i}
			}
			flush(classBare)
			parts = append(parts, wordPart{val: q.String(), class: classDouble})
			i = j + 1
		case '\\':
			if i+1 < n {
				b.WriteByte(src[i+1])
				i += 2
			} else {
				return token{}, 0, &lexError{msg: "trailing backslash", pos: i}
			}
		case '{', '}':
			b.WriteByte(c)
			i++
		default:
			b.WriteByte(c)
			i++
		}
	}
	flush(classBare)
	return token{kind: tokWord, parts: parts, pos: i}, i, nil
}
