package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
)

const (
	syncBufferCap   = 8 << 20 // 8MB cap on buffered output before truncation
	eventOutputTail = 16 << 10
)

type JobCompletion struct {
	JobID      string `json:"job_id"`
	Command    string `json:"command"`
	ExitCode   int    `json:"exit_code"`
	Stdout     string `json:"stdout"`
	Stderr     string `json:"stderr"`
	Truncated  bool   `json:"truncated"`
	StartedAt  int64  `json:"started_at"`
	FinishedAt int64  `json:"finished_at"`
}

type Manager struct {
	store    *Store
	sess     *Session
	mu       sync.Mutex
	runners  map[string]*Runner
	onFinish func(JobCompletion)
}

func NewManager(s *Store, workdir string) *Manager {
	return &Manager{store: s, sess: NewSession(workdir), runners: map[string]*Runner{}}
}

func (m *Manager) SetCompletionHandler(fn func(JobCompletion)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onFinish = fn
}

func randomJobID() string {
	b := make([]byte, 3)
	_, _ = rand.Read(b)
	return "j" + hex.EncodeToString(b)
}

// Runner executes one parsed command line. Fast completion returns the
// merged output without touching the job store; promotion (explicitly via
// Promote) registers the job so job_output/kill/stdin/wait work on it.
type Runner struct {
	mgr      *Manager
	segs     []segment
	started  int64
	done     chan struct{}
	exit     int
	exitOnce sync.Once

	mu        sync.Mutex
	rows      []OutputRow // interleaved buffered rows
	truncated bool
	promoted  bool
	jobID     string
	proc      *os.Process
	stdinW    *os.File
	finished  bool

	sw *rowWriter
	ew *rowWriter
}

func (m *Manager) StartLine(command string) (*Runner, string) {
	segs, err := parseSegments(command)
	if err != nil {
		return nil, err.Error()
	}
	r := &Runner{mgr: m, segs: segs, started: time.Now().Unix(), done: make(chan struct{})}
	r.sw = &rowWriter{r: r, stream: "stdout"}
	r.ew = &rowWriter{r: r, stream: "stderr"}
	go r.run()
	return r, ""
}

func (r *Runner) appendRow(stream, content string) {
	if content == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.truncated {
		return
	}
	total := 0
	for _, row := range r.rows {
		total += len(row.Content)
	}
	if total+len(content) > syncBufferCap {
		r.truncated = true
		r.rows = append(r.rows, OutputRow{Content: "\n[output truncated at 8MB]\n", Stream: "stderr"})
		if r.promoted {
			r.mgr.store.AppendRow(r.jobID, "stderr", "\n[output truncated at 8MB]\n")
		}
		return
	}
	r.rows = append(r.rows, OutputRow{Content: content, Stream: stream, TS: time.Now().UnixNano()})
	if r.promoted {
		r.mgr.store.AppendRow(r.jobID, stream, content)
	}
}

func (r *Runner) stdoutWriter() io.Writer { return r.sw }
func (r *Runner) stderrWriter() io.Writer { return r.ew }

// rowWriter buffers incoming bytes and emits only complete, newline-terminated
// lines to the runner's row store. A trailing fragment (no '\n' yet) is held in
// carry and prepended to the next write, so stored rows are always aligned to
// whole lines and line-number pagination stays stable across polls.
type rowWriter struct {
	r      *Runner
	stream string
	carry  []byte
}

func (w *rowWriter) Write(p []byte) (int, error) {
	data := append(w.carry, p...)
	w.carry = nil
	for {
		idx := bytes.IndexByte(data, '\n')
		if idx < 0 {
			if len(data) > 0 {
				w.carry = append(w.carry, data...)
			}
			break
		}
		line := data[:idx+1]
		w.r.appendRow(w.stream, string(line))
		data = data[idx+1:]
	}
	return len(p), nil
}

func (w *rowWriter) flush() {
	if len(w.carry) > 0 {
		w.r.appendRow(w.stream, string(w.carry))
		w.carry = nil
	}
}

func (r *Runner) lookupVar(name string) string {
	if v, ok := r.mgr.sess.lookup(name); ok {
		return v
	}
	return os.Getenv(name)
}

func (r *Runner) truncFlag() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.truncated
}

func (r *Runner) run() {
	lastExit := 0
	var lastErr string
	for _, seg := range r.segs {
		if seg.op == opAnd && lastExit != 0 {
			continue
		}
		if seg.op == opOr && lastExit == 0 {
			continue
		}
		argv := []string{}
		skip := false
		for _, w := range seg.parts {
			words, lerr := expandWord(w, r.lookupVar, r.mgr.sess.Dir())
			if lerr != nil {
				lastExit = 2
				lastErr = "sh: " + lerr.Error()
				r.appendRow("stderr", lastErr+"\n")
				skip = true
				break
			}
			argv = append(argv, words...)
		}
		if skip {
			continue
		}
		if len(argv) == 0 {
			continue
		}
		if isBuiltin, code, msg := runBuiltin(argv, r.mgr.sess); isBuiltin {
			lastExit = code
			if msg != "" {
				r.appendRow("stderr", msg+"\n")
			}
			continue
		}
		exit, errStr := r.execProcess(argv)
		lastExit = exit
		lastErr = errStr
	}
	r.mu.Lock()
	r.exit = lastExit
	r.finished = true
	promoted := r.promoted
	r.mu.Unlock()
	r.exitOnce.Do(func() { close(r.done) })
	if promoted {
		r.mgr.finalizeRunner(r, lastExit, lastErr)
	}
}

func (r *Runner) execProcess(argv []string) (int, string) {
	cmd := newExecCmd(argv, r.mgr.sess)
	stdinR, stdinW, perr := os.Pipe()
	if perr == nil {
		cmd.Stdin = stdinR
	}
	cmd.Stdout = r.stdoutWriter()
	cmd.Stderr = r.stderrWriter()
	if err := cmd.Start(); err != nil {
		if stdinW != nil {
			stdinW.Close()
		}
		if stdinR != nil {
			stdinR.Close()
		}
		r.appendRow("stderr", argv[0]+": "+err.Error()+"\n")
		return 127, ""
	}
	r.mu.Lock()
	r.proc = cmd.Process
	r.stdinW = stdinW
	r.mu.Unlock()
	waitErr := cmd.Wait()
	// Flush any trailing partial line from both writers.
	r.stdoutFlush()
	r.stderrFlush()
	if stdinW != nil {
		stdinW.Close()
	}
	if stdinR != nil {
		stdinR.Close()
	}
	r.mu.Lock()
	r.proc = nil
	r.stdinW = nil
	r.mu.Unlock()
	if waitErr == nil {
		return 0, ""
	}
	if ee, ok := waitErr.(*exec.ExitError); ok {
		ws := ee.Sys().(syscall.WaitStatus)
		if ws.Signaled() {
			return 128 + int(ws.Signal()), ""
		}
		return ws.ExitStatus(), ""
	}
	return 1, waitErr.Error()
}

// Wait blocks up to timeout for completion.
func (r *Runner) Wait(timeout time.Duration) bool {
	select {
	case <-r.done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// Promote registers the runner as a job and returns its jid.
func (r *Runner) Promote() string {
	r.mu.Lock()
	if r.promoted {
		jid := r.jobID
		r.mu.Unlock()
		return jid
	}
	r.promoted = true
	jid := randomJobID()
	r.jobID = jid
	rows := append([]OutputRow(nil), r.rows...)
	finished := r.finished
	exit := r.exit
	r.mu.Unlock()

	r.mgr.store.InsertJob(&Job{ID: jid, Command: r.plainCommand(), State: "running", ExitCode: -1, StartedAt: r.started})
	for _, row := range rows {
		r.mgr.store.AppendRow(jid, row.Stream, row.Content)
	}
	r.mgr.mu.Lock()
	r.mgr.runners[jid] = r
	r.mgr.mu.Unlock()
	if finished {
		r.mgr.finalizeRunner(r, exit, "")
	}
	return jid
}

func (r *Runner) plainCommand() string {
	var out string
	for i, seg := range r.segs {
		if i > 0 {
			switch seg.op {
			case opAnd:
				out += " && "
			case opOr:
				out += " || "
			default:
				out += "; "
			}
		}
		for _, w := range seg.parts {
			out += plainWord(w) + " "
		}
	}
	return out
}

// Output returns the interleaved merged output collected so far.
func (r *Runner) Output() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var b []byte
	for _, row := range r.rows {
		b = append(b, row.Content...)
	}
	return string(b)
}

// stdoutFlush / stderrFlush flush any buffered partial line on the writer so
// synchronous execute output is not missing the final unterminated line.
func (r *Runner) stdoutFlush() {
	if r.sw != nil {
		r.sw.flush()
	}
}
func (r *Runner) stderrFlush() {
	if r.ew != nil {
		r.ew.flush()
	}
}

func (r *Runner) ExitCode() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.exit
}

func (r *Runner) Finished() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.finished
}

// WriteStdin sends data to the currently running process's stdin.
func (r *Runner) WriteStdin(data []byte, closeAfter bool) (int, error) {
	r.mu.Lock()
	w := r.stdinW
	r.mu.Unlock()
	if w == nil {
		return 0, errNoStdin
	}
	n, err := w.Write(data)
	if closeAfter {
		w.Close()
	}
	return n, err
}

var errNoStdin = &stdinError{}

type stdinError struct{}

func (e *stdinError) Error() string { return "job is not currently reading stdin" }

// Kill terminates the whole process group of the running process.
func (r *Runner) Kill() bool {
	r.mu.Lock()
	p := r.proc
	r.mu.Unlock()
	if p == nil {
		return false
	}
	_ = syscall.Kill(-p.Pid, syscall.SIGKILL)
	_ = syscall.Kill(p.Pid, syscall.SIGKILL)
	return true
}

func (m *Manager) finalizeRunner(r *Runner, exit int, _ string) {
	r.mu.Lock()
	jid := r.jobID
	r.mu.Unlock()
	if jid == "" {
		return
	}
	wasKilled := m.store.GetJob(jid) != nil && m.store.GetJob(jid).State == "killed"
	state := "failed"
	code := exit
	if wasKilled {
		state = "killed"
		code = -9
	} else if exit == 0 {
		state = "done"
	}
	now := time.Now().Unix()
	m.store.UpdateJobState(jid, state, code, now)
	m.mu.Lock()
	handler := m.onFinish
	m.mu.Unlock()
	if handler != nil {
		j := m.store.GetJob(jid)
		if j != nil {
			stdout, stderr, trunc := m.store.Tails(jid, eventOutputTail)
			handler(JobCompletion{
				JobID:      jid,
				Command:    j.Command,
				ExitCode:   code,
				Stdout:     stdout,
				Stderr:     stderr,
				Truncated:  trunc,
				StartedAt:  j.StartedAt,
				FinishedAt: now,
			})
		}
	}
	m.mu.Lock()
	delete(m.runners, jid)
	m.mu.Unlock()
}

func (s *Session) lookup(name string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.env[name]
	return v, ok
}

func (m *Manager) KillAll() {
	m.mu.Lock()
	rs := make([]*Runner, 0, len(m.runners))
	for _, r := range m.runners {
		rs = append(rs, r)
	}
	m.mu.Unlock()
	for _, r := range rs {
		r.Kill()
	}
}

// streamCopy remains for piping raw readers (unused by runner; kept for
// compatibility with any direct store writers).
func streamCopy(reader io.Reader, store *Store, jobID, stream string) {
	buf := make([]byte, 4096)
	var carry []byte
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			data := append(carry, buf[:n]...)
			cut := len(data)
			for cut > 0 && !utf8.Valid(data[:cut]) {
				cut--
			}
			if cut > 0 {
				store.AppendRow(jobID, stream, string(data[:cut]))
			}
			carry = append(carry[:0], data[cut:]...)
		}
		if err != nil {
			if len(carry) > 0 {
				store.AppendRow(jobID, stream, string(carry))
			}
			return
		}
	}
}

// KillJob kills a runner by jid if still running.
func (m *Manager) KillJob(jid string) bool {
	m.mu.Lock()
	r := m.runners[jid]
	m.mu.Unlock()
	if r == nil {
		return false
	}
	ok := r.Kill()
	if ok {
		m.store.MarkKilled(jid)
	}
	return ok
}
