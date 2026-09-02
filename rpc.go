package main

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const backgroundNote = "job %s is running; inspect output with job_output (offset/limit/grep), list jobs with job_list, wait via job_wait, send input via job_stdin, stop via job_kill"

func (h *wsHub) cmdExecute(params map[string]json.RawMessage) (any, string) {
	var p struct {
		Command string  `json:"command"`
		Rev     *string `json:"rev"`
	}
	if err := decodeParams(params, &p); err != "" {
		return nil, err
	}
	if p.Command == "" {
		return nil, "execute params: command is required"
	}
	if p.Rev != nil && h.state.syncedRev != *p.Rev {
		return map[string]any{"need_sync": true}, ""
	}

	runner, parseErr := h.state.jobs.StartLine(p.Command)
	if parseErr != "" {
		return nil, "shell: " + parseErr
	}

	// Every command is registered as a job (no fast/slow split): the caller
	// subscribes to the per-job SSE stream for output, which replays history
	// for instantly-finished commands and streams live deltas for long ones.
	jid, perr := runner.Promote()
	if perr != nil {
		return nil, perr.Error()
	}
	return map[string]any{
		"job_id":       jid,
		"backgrounded": true,
		"note":         strings.ReplaceAll(backgroundNote, "%s", jid),
	}, ""
}

// resolveSandboxed maps an RPC path (relative or absolute) onto the worker
// workspace and rejects anything that escapes it.
func resolveSandboxed(root, path string) (string, string) {
	if path == "" {
		return "", "path is required"
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(root, path)
	} else {
		path = filepath.Clean(path)
	}
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == "" || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", "path escapes workspace: " + path
	}
	return path, ""
}

func (h *wsHub) cmdFileRead(params map[string]json.RawMessage) (any, string) {
	var p struct {
		Path string `json:"path"`
	}
	if err := decodeParams(params, &p); err != "" {
		return nil, err
	}
	full, perr := resolveSandboxed(h.state.workdir, p.Path)
	if perr != "" {
		return nil, perr
	}
	b, e := os.ReadFile(full)
	if e != nil {
		return nil, "file_read: " + e.Error()
	}
	return map[string]any{"content": base64.StdEncoding.EncodeToString(b)}, ""
}

func (h *wsHub) cmdFileWrite(params map[string]json.RawMessage) (any, string) {
	var p struct {
		Path    string `json:"path"`
		Content string `json:"content"`
	}
	if err := decodeParams(params, &p); err != "" {
		return nil, err
	}
	full, perr := resolveSandboxed(h.state.workdir, p.Path)
	if perr != "" {
		return nil, perr
	}
	data, err := base64.StdEncoding.DecodeString(p.Content)
	if err != nil {
		return nil, "file_write: " + err.Error()
	}
	parent := filepath.Dir(full)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return nil, err.Error()
	}
	if err := os.WriteFile(full, data, 0o644); err != nil {
		return nil, "file_write: " + err.Error()
	}
	return map[string]any{"ok": true, "path": p.Path}, ""
}

func (h *wsHub) cmdFileDelete(params map[string]json.RawMessage) (any, string) {
	var p struct {
		Path string `json:"path"`
	}
	if err := decodeParams(params, &p); err != "" {
		return nil, err
	}
	full, perr := resolveSandboxed(h.state.workdir, p.Path)
	if perr != "" {
		return nil, perr
	}
	if err := os.RemoveAll(full); err != nil {
		return nil, "file_delete: " + err.Error()
	}
	return map[string]any{"ok": true, "path": p.Path}, ""
}

// cmdFileList walks the sandbox path (a file or directory) and returns every
// file under it as {path, size, content(base64)} — relative to the workspace
// root. Used by sandbox-port to expand a directory into a batch of file edits.
func (h *wsHub) cmdFileList(params map[string]json.RawMessage) (any, string) {
	var p struct {
		Path string `json:"path"`
	}
	if err := decodeParams(params, &p); err != "" {
		return nil, err
	}
	full, perr := resolveSandboxed(h.state.workdir, p.Path)
	if perr != "" {
		return nil, perr
	}
	info, err := os.Stat(full)
	if err != nil {
		return nil, "file_list: " + err.Error()
	}
	var rels []string
	var single bool
	if info.IsDir() {
		rels = listWorkspaceFiles(full)
	} else {
		rels = []string{filepath.Base(full)}
		single = true
	}
	out := []map[string]any{}
	for _, rel := range rels {
		abs := full
		if !single {
			abs = filepath.Join(full, rel)
		}
		data, err := os.ReadFile(abs)
		if err != nil {
			return nil, "file_list: read " + rel + ": " + err.Error()
		}
		// All returned paths are relative to the sandbox path given.
		out = append(out, map[string]any{
			"path":    rel,
			"size":    len(data),
			"content": base64.StdEncoding.EncodeToString(data),
		})
	}
	return map[string]any{"files": out, "is_dir": info.IsDir()}, ""
}

func (h *wsHub) cmdJobs(_ map[string]json.RawMessage) (any, string) {
	jobs := make([]map[string]any, 0)
	for _, j := range h.state.store.ListJobs() {
		jobs = append(jobs, map[string]any{
			"id": j.ID, "command": j.Command, "state": j.State,
			"exit_code": j.ExitCode, "started_at": j.StartedAt, "finished_at": j.FinishedAt,
		})
	}
	return map[string]any{"jobs": jobs}, ""
}

func (h *wsHub) cmdJobOutput(params map[string]json.RawMessage) (any, string) {
	var p struct {
		JobID  string `json:"job_id"`
		Stream string `json:"stream"`
		Offset int    `json:"offset"`
		Limit  int    `json:"limit"`
		Grep   string `json:"grep"`
		Invert bool   `json:"invert"`
		Regex  bool   `json:"regex"`
	}
	if err := decodeParams(params, &p); err != "" {
		return nil, err
	}
	if p.Stream == "" {
		p.Stream = "all"
	}
	rows := h.state.store.LinesFor(p.JobID, p.Stream, p.Grep, p.Invert, p.Regex, p.Offset, p.Limit)
	if rows == nil {
		return nil, "job not found"
	}
	return map[string]any{
		"lines": rows.Lines, "total_lines": rows.TotalLines,
		"start_line": rows.StartLine, "end_line": rows.EndLine, "done": rows.Done,
	}, ""
}

func (h *wsHub) cmdJobWait(params map[string]json.RawMessage) (any, string) {
	var p struct {
		JobID     string `json:"job_id"`
		TimeoutMs int    `json:"timeout_ms"`
	}
	if err := decodeParams(params, &p); err != "" {
		return nil, err
	}
	if p.JobID == "" {
		return nil, "job_id is required"
	}
	if p.TimeoutMs <= 0 {
		p.TimeoutMs = 30_000
	}
	// Cap at 60s: the caller's WS read deadline (ops-extension CommandOnce)
	// is 65s, so a 60s wait must always respond before the client gives up.
	const jobWaitMaxMs = 60_000
	if p.TimeoutMs > jobWaitMaxMs {
		p.TimeoutMs = jobWaitMaxMs
	}

	h.state.jobs.mu.Lock()
	runner := h.state.jobs.runners[p.JobID]
	h.state.jobs.mu.Unlock()

	if runner == nil {
		j := h.state.store.GetJob(p.JobID)
		if j == nil {
			return nil, "job not found"
		}
		return map[string]any{"state": j.State, "exit_code": j.ExitCode, "waited": false}, ""
	}

	finished := runner.Wait(time.Duration(p.TimeoutMs) * time.Millisecond)
	state := "running"
	exit := -1
	if j := h.state.store.GetJob(p.JobID); j != nil && j.State != "running" {
		state = j.State
		exit = j.ExitCode
	} else if finished {
		state = "done"
		exit = runner.ExitCode()
	}
	return map[string]any{"state": state, "exit_code": exit, "waited": finished}, ""
}

func (h *wsHub) cmdKill(params map[string]json.RawMessage) (any, string) {
	var p struct {
		JobID string `json:"job_id"`
	}
	if err := decodeParams(params, &p); err != "" {
		return nil, err
	}
	h.state.jobs.mu.Lock()
	runner := h.state.jobs.runners[p.JobID]
	h.state.jobs.mu.Unlock()
	if runner == nil {
		return map[string]any{"ok": false}, ""
	}
	ok := runner.Kill()
	if ok {
		h.state.store.MarkKilled(p.JobID)
	}
	return map[string]any{"ok": ok}, ""
}

func (h *wsHub) cmdJobStdin(params map[string]json.RawMessage) (any, string) {
	var p struct {
		JobID string `json:"job_id"`
		Data  string `json:"data"`
		Close bool   `json:"close"`
	}
	if err := decodeParams(params, &p); err != "" {
		return nil, err
	}
	if p.JobID == "" {
		return nil, "job_id is required"
	}
	if len(p.Data) > 256<<10 {
		return nil, "stdin data exceeds 256KB per call"
	}
	h.state.jobs.mu.Lock()
	runner := h.state.jobs.runners[p.JobID]
	h.state.jobs.mu.Unlock()
	if runner == nil {
		j := h.state.store.GetJob(p.JobID)
		if j == nil {
			return nil, "job not found"
		}
		return nil, "job is not running"
	}
	// The runner's stdin pipe is installed asynchronously by the execution
	// goroutine (cmd.Start() precedes the write-end handoff). A job_stdin
	// arriving right after a backgrounded execute may outrun that handoff:
	// poll briefly instead of failing with "not reading stdin".
	var n int
	var werr error
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		n, werr = runner.WriteStdin([]byte(p.Data), p.Close)
		if werr != errNoStdin || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if werr != nil {
		return nil, "job_stdin: " + werr.Error()
	}
	return map[string]any{"written": n, "closed": p.Close}, ""
}

func decodeParams(params map[string]json.RawMessage, out any) string {
	if len(params) == 0 {
		if err := json.Unmarshal([]byte("{}"), out); err != nil {
			return err.Error()
		}
		return ""
	}
	b, err := json.Marshal(params)
	if err != nil {
		return err.Error()
	}
	if err := json.Unmarshal(b, out); err != nil {
		return err.Error()
	}
	return ""
}
