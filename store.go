package main

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
)

// Job store retention: finished jobs are evicted once their count exceeds
// maxJobHistory; running jobs are never evicted, but a soft cap on running
// jobs (maxRunningJobs) defends against zombie-process accumulation.
const maxJobHistory = 500
const maxRunningJobs = 1000

type Job struct {
	ID         string  `json:"id"`
	Command    string  `json:"command"`
	State      string  `json:"state"`
	ExitCode   int     `json:"exit_code"`
	ParentID   *string `json:"parent_id"`
	StartedAt  int64   `json:"started_at"`
	FinishedAt *int64  `json:"finished_at"`
}

type OutputRow struct {
	Content string
	Stream  string // stdout | stderr
	TS      int64
}

// Store keeps jobs and a single interleaved (arrival-ordered) output log
// per job; stream views filter on read.
type Store struct {
	mu     sync.RWMutex
	jobs   map[string]*Job
	order  []string
	output map[string][]OutputRow
}

func NewStore() *Store {
	return &Store{
		jobs:   map[string]*Job{},
		output: map[string][]OutputRow{},
	}
}

// InsertJob registers a job. Running jobs are never evicted, but are refused
// beyond maxRunningJobs (zombie defence). Finished jobs are evicted oldest-
// finish-first once they exceed maxJobHistory.
func (s *Store) InsertJob(j *Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if j.State == "running" && s.countRunningLocked() >= maxRunningJobs {
		return fmt.Errorf("too many running jobs (%d)", maxRunningJobs)
	}

	s.jobs[j.ID] = j
	s.order = append(s.order, j.ID)
	s.evictFinishedLocked()
	return nil
}

func (s *Store) countRunningLocked() int {
	n := 0
	for _, id := range s.order {
		if j, ok := s.jobs[id]; ok && j.State == "running" {
			n++
		}
	}
	return n
}

// evictFinishedLocked removes finished jobs (oldest finish first) beyond
// maxJobHistory. Running jobs are skipped and never removed here.
func (s *Store) evictFinishedLocked() {
	type finished struct {
		id     string
		finish int64
	}
	var finishedJobs []finished
	for _, id := range s.order {
		j, ok := s.jobs[id]
		if !ok || j.State == "running" {
			continue
		}
		fin := int64(0)
		if j.FinishedAt != nil {
			fin = *j.FinishedAt
		}
		finishedJobs = append(finishedJobs, finished{id: id, finish: fin})
	}
	if len(finishedJobs) <= maxJobHistory {
		return
	}
	sort.Slice(finishedJobs, func(i, k int) bool {
		return finishedJobs[i].finish < finishedJobs[k].finish
	})
	excess := len(finishedJobs) - maxJobHistory
	drop := map[string]bool{}
	for i := 0; i < excess; i++ {
		drop[finishedJobs[i].id] = true
	}
	kept := s.order[:0]
	for _, id := range s.order {
		if drop[id] {
			delete(s.jobs, id)
			delete(s.output, id)
			continue
		}
		kept = append(kept, id)
	}
	s.order = kept
}

func (s *Store) GetJob(id string) *Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if j, ok := s.jobs[id]; ok {
		c := *j
		return &c
	}
	return nil
}

func (s *Store) UpdateJobState(id, state string, exitCode int, finishedAt int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if j, ok := s.jobs[id]; ok {
		j.State = state
		j.ExitCode = exitCode
		j.FinishedAt = &finishedAt
	}
}

func (s *Store) MarkKilled(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if j, ok := s.jobs[id]; ok && j.State == "running" {
		j.State = "killed"
		j.ExitCode = -9
	}
}

func (s *Store) AppendRow(jobID, stream, content string) {
	if content == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.output[jobID] = append(s.output[jobID], OutputRow{Content: content, Stream: stream})
}

func (s *Store) ListJobs() []Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Job, 0, len(s.order))
	for i := len(s.order) - 1; i >= 0; i-- {
		if j, ok := s.jobs[s.order[i]]; ok {
			out = append(out, *j)
		}
	}
	return out
}

func (s *Store) rowsFor(jobID, stream string) []OutputRow {
	rows := s.output[jobID]
	if stream == "" || stream == "all" {
		return rows
	}
	filtered := make([]OutputRow, 0, len(rows))
	for _, r := range rows {
		if r.Stream == stream {
			filtered = append(filtered, r)
		}
	}
	return filtered
}

// Rows returns a snapshot of a job's interleaved output rows, for stream
// history replay.
func (s *Store) Rows(jobID string) []OutputRow {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]OutputRow(nil), s.output[jobID]...)
}

func (s *Store) OutputRaw(jobID, stream string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var b []byte
	for _, r := range s.rowsFor(jobID, stream) {
		b = append(b, r.Content...)
	}
	return string(b)
}

// Tails returns the last cap bytes of each stream plus a truncation flag,
// used for the job.completed event.
func (s *Store) Tails(jobID string, cap int) (string, string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stdout := s.concatLocked(jobID, "stdout")
	stderr := s.concatLocked(jobID, "stderr")
	trunc := false
	if len(stdout) > cap {
		stdout = stdout[len(stdout)-cap:]
		trunc = true
	}
	if len(stderr) > cap {
		stderr = stderr[len(stderr)-cap:]
		trunc = true
	}
	return stdout, stderr, trunc
}

func (s *Store) concatLocked(jobID, stream string) string {
	var b []byte
	for _, r := range s.rowsFor(jobID, stream) {
		b = append(b, r.Content...)
	}
	return string(b)
}

type OutputRowsResult struct {
	Lines      []string `json:"lines"`
	TotalLines int      `json:"total_lines"`
	StartLine  int      `json:"start_line"`
	EndLine    int      `json:"end_line"`
	Done       bool     `json:"done"`
}

// LinesFor returns job output lines with filtering (grep/regex/invert)
// followed by offset/limit pagination. offset may be negative (from the
// end). limit defaults to 200, capped at 500.
func (s *Store) LinesFor(jobID, stream, grep string, invert, regex bool, offset, limit int) *OutputRowsResult {
	j := s.GetJob(jobID)
	if j == nil {
		return nil
	}
	s.mu.RLock()
	raw := s.rowsFor(jobID, stream)
	s.mu.RUnlock()

	lines := []string{}
	var b []byte
	for _, r := range raw {
		b = append(b, r.Content...)
	}
	lines = splitLines(string(b))
	if grep != "" {
		var re *regexp.Regexp
		if regex {
			compiled, err := regexp.Compile("(?i)" + grep)
			if err != nil {
				return &OutputRowsResult{Lines: []string{}, TotalLines: 0, StartLine: 0, EndLine: 0, Done: false}
			}
			re = compiled
		}
		var kept []string
		for _, l := range lines {
			var match bool
			if re != nil {
				match = re.MatchString(l)
			} else {
				match = strings.Contains(strings.ToLower(l), strings.ToLower(grep))
			}
			if match != invert {
				kept = append(kept, l)
			}
		}
		lines = kept
	}

	total := len(lines)
	if limit <= 0 {
		limit = 200
	}
	if limit > 500 {
		limit = 500
	}
	start := offset
	if start < 0 {
		start = total + start
	}
	if start < 0 {
		start = 0
	}
	if start > total {
		return &OutputRowsResult{Lines: []string{}, TotalLines: total, StartLine: start, EndLine: start - 1, Done: j.State != "running"}
	}
	end := start + limit
	if end > total {
		end = total
	}
	page := lines[start:end]
	if page == nil {
		page = []string{}
	}
	return &OutputRowsResult{
		Lines:      page,
		TotalLines: total,
		StartLine:  start,
		EndLine:    end - 1,
		Done:       j.State != "running",
	}
}

// splitLines splits s into lines. Rows are stored newline-terminated by the
// rowWriter, so a trailing "" (from a final '\n') is dropped; a genuine
// unterminated trailing fragment (only possible for pre-migration data) is kept
// as-is so it is never silently lost.
func splitLines(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, "\n")
	if parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts
}
