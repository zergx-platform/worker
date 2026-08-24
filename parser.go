package main

import "strings"

type segOp int

const (
	opThen segOp = iota // ';'
	opAnd               // '&&'
	opOr                // '||'
)

type segment struct {
	parts [][]wordPart // argv before expansion
	op    segOp        // operator that PRECEDES this segment (first = opThen)
}

// parseSegments splits a token stream into command segments with their
// joining operators. It validates that control keywords never appear as a
// command word and that segments are non-empty.
func parseSegments(src string) ([]segment, error) {
	toks, err := tokenize(src)
	if err != nil {
		return nil, err
	}
	var segs []segment
	cur := segment{op: opThen}
	for _, t := range toks {
		switch t.kind {
		case tokWord:
			cur.parts = append(cur.parts, t.parts)
		case tokSep:
			if len(cur.parts) == 0 {
				continue
			}
			segs = append(segs, cur)
			cur = segment{op: opThen}
		case tokAnd:
			if len(cur.parts) == 0 {
				return nil, &lexError{msg: "dangling '&&'", pos: t.pos}
			}
			segs = append(segs, cur)
			cur = segment{op: opAnd}
		case tokOr:
			if len(cur.parts) == 0 {
				return nil, &lexError{msg: "dangling '||'", pos: t.pos}
			}
			segs = append(segs, cur)
			cur = segment{op: opOr}
		case tokEOL:
			if len(cur.parts) > 0 {
				segs = append(segs, cur)
			}
		}
	}
	for _, s := range segs {
		w := plainWord(s.parts[0])
		if controlWords[w] {
			return nil, &lexError{msg: errCtrlWord(w)}
		}
	}
	if len(segs) == 0 {
		return nil, &lexError{msg: "empty command"}
	}
	return segs, nil
}

func errCtrlWord(w string) string {
	return "\"" + w + "\" is a shell control construct: this is a minimal shell, issue commands one at a time joined by && || or ;"
}

func plainWord(parts []wordPart) string {
	var b strings.Builder
	for _, p := range parts {
		b.WriteString(p.val)
	}
	return b.String()
}
