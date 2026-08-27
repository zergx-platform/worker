package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func execResult(t *testing.T, c *websocket.Conn, id int, rev, cmd string) map[string]any {
	t.Helper()
	return rpcCall(t, c, id, "execute", map[string]any{"command": cmd, "rev": rev})["result"].(map[string]any)
}

// execWait runs a command, waits for its job to finish, and returns the
// merged output + exit code (the post-split contract: execute yields a job,
// output is fetched via job_output).
func execWait(t *testing.T, c *websocket.Conn, id int, rev, cmd string) (string, int) {
	t.Helper()
	r := execResult(t, c, id, rev, cmd)
	jid, _ := r["job_id"].(string)
	if jid == "" {
		t.Fatalf("execute must return job_id, got %v", r)
	}
	w := rpcCall(t, c, id+1000, "job_wait", map[string]any{"job_id": jid, "timeout_ms": 5000})
	wres := w["result"].(map[string]any)
	state, _ := wres["state"].(string)
	if state != "done" && state != "failed" && state != "killed" {
		t.Fatalf("job_wait = %v, want terminal state", wres)
	}
	o := rpcCall(t, c, id+2000, "job_output", map[string]any{"job_id": jid, "stream": "all", "offset": 0, "limit": 500})
	ores := o["result"].(map[string]any)
	lines, _ := ores["lines"].([]any)
	var sb strings.Builder
	for _, l := range lines {
		sb.WriteString(l.(string))
	}
	exit := -1
	if j := rpcCall(t, c, id+3000, "jobs", map[string]any{}); true {
		jres := j["result"].(map[string]any)["jobs"].([]any)
		for _, e := range jres {
			m := e.(map[string]any)
			if m["id"] == jid {
				exit = int(m["exit_code"].(float64))
			}
		}
	}
	return sb.String(), exit
}

func TestShellRejectMatrix(t *testing.T) {
	srv, _ := newTestServer(t)
	c := dialWs(t, srv)
	setStateRevHttp(t, srv, "rev-r")

	cases := []struct {
		cmd     string
		wantErr string
	}{
		{"ls | head -3", "pipes are not supported"},
		{"ls |& wc", "pipes are not supported"},
		{"sleep 5 &", "background '&' is not supported"},
		{"echo hi > out.txt", "redirection is not supported"},
		{"echo hi >> out.txt", "redirection are not supported"},
		{"cat < in.txt", "redirection are not supported"},
		{"echo hi 2>&1", "redirection are not supported"},
		{"echo $(whoami)", "command substitution"},
		{"echo `whoami`", "command substitution"},
		{"(cd /tmp && ls)", "subshells/grouping"},
		{"{ ls; }", "subshells/grouping"},
		{"for i in 1 2; do echo $i; done", "control construct"},
		{"if true; then echo x; fi", "control construct"},
	}
	for i, tc := range cases {
		resp := rpcCall(t, c, i+1, "execute", map[string]any{"command": tc.cmd, "rev": "rev-r"})
		errMsg, _ := resp["error"].(string)
		if errMsg == "" {
			t.Fatalf("cmd %q must be rejected, got %v", tc.cmd, resp["result"])
		}
		if !strings.Contains(errMsg, "shell:") {
			t.Fatalf("cmd %q error should be namespaced: %v", tc.cmd, errMsg)
		}
	}
}

func TestShellAcceptMatrix(t *testing.T) {
	srv, _ := newTestServer(t)
	c := dialWs(t, srv)
	setStateRevHttp(t, srv, "rev-a")

	o1, _ := execWait(t, c, 1, "rev-a", "echo one && echo two")
	if !strings.Contains(o1, "one") || !strings.Contains(o1, "two") {
		t.Fatalf("&& chain output = %q", o1)
	}

	o2, _ := execWait(t, c, 2, "rev-a", "false || echo fallback")
	if !strings.Contains(o2, "fallback") {
		t.Fatalf("|| fallback output = %q", o2)
	}

	o3, _ := execWait(t, c, 3, "rev-a", "echo 'a|b>c&d'")
	if !strings.Contains(o3, "a|b>c&d") {
		t.Fatalf("quoted metachars must stay literal, got %q", o3)
	}

	writeTestFile(t, "m.txt", "alpha\nbeta\ngamma\n")
	o4, _ := execWait(t, c, 4, "rev-a", "grep -E 'lph|mm' m.txt")
	if !strings.Contains(o4, "alpha") || !strings.Contains(o4, "gamma") || strings.Contains(o4, "beta") {
		t.Fatalf("quoted regex pipe must pass through, got %q", o4)
	}

	o5, _ := execWait(t, c, 5, "rev-a", "echo x\necho y")
	if !strings.Contains(o5, "x") || !strings.Contains(o5, "y") {
		t.Fatalf("newline as ; output = %q", o5)
	}

	// third-party commands stay allowed (no command blacklist).
	_, ec6 := execWait(t, c, 6, "rev-a", "kill -l")
	if ec6 != 0 {
		t.Fatalf("kill -l exit = %d, want 0", ec6)
	}

	// variable-expansion-time rejections surface as exit 2 + guidance.
	for _, cmd := range []string{"echo $?", "echo $$"} {
		oo, ec := execWait(t, c, 7, "rev-a", cmd)
		if ec != 2 {
			t.Fatalf("cmd %q exit = %v, want 2", cmd, ec)
		}
		if !strings.Contains(oo, "special shell parameters") {
			t.Fatalf("cmd %q output = %q, want guidance", cmd, oo)
		}
	}
}

func TestSessionPersistence(t *testing.T) {
	srv, _ := newTestServer(t)
	c := dialWs(t, srv)
	setStateRevHttp(t, srv, "rev-s")

	execWait(t, c, 1, "rev-s", "export GREETING=hello-world")
	o, _ := execWait(t, c, 2, "rev-s", "echo $GREETING")
	if !strings.Contains(o, "hello-world") {
		t.Fatalf("export must persist across executes, got %q", o)
	}

	execWait(t, c, 3, "rev-s", "unset GREETING")
	o2, _ := execWait(t, c, 4, "rev-s", "echo [$GREETING]")
	if !strings.Contains(o2, "[]") {
		t.Fatalf("unset must remove the variable, got %q", o2)
	}

	writeTestFile(t, "sub/deep.txt", "x")
	execWait(t, c, 5, "rev-s", "cd sub")
	o3, _ := execWait(t, c, 6, "rev-s", "cat deep.txt")
	if !strings.Contains(o3, "x") {
		t.Fatalf("cd must persist across executes, got %q", o3)
	}
	execWait(t, c, 7, "rev-s", "cd ..")
}

func TestBackgroundedPromotion(t *testing.T) {
	srv, _ := newTestServer(t)
	c := dialWs(t, srv)
	setStateRevHttp(t, srv, "rev-b")

	r := execResult(t, c, 1, "rev-b", "sleep 1")
	jid, _ := r["job_id"].(string)
	if jid == "" || r["backgrounded"] != true {
		t.Fatalf("slow command must be backgrounded, got %v", r)
	}

	w := rpcCall(t, c, 2, "job_wait", map[string]any{"job_id": jid, "timeout_ms": 5000})
	wres := w["result"].(map[string]any)
	if wres["state"] != "done" || wres["waited"] != true {
		t.Fatalf("job_wait = %v", wres)
	}

	jl := rpcCall(t, c, 3, "jobs", map[string]any{})
	jobs := jl["result"].(map[string]any)["jobs"].([]any)
	found := false
	for _, j := range jobs {
		if j.(map[string]any)["id"] == jid {
			found = true
		}
	}
	if !found {
		t.Fatalf("backgrounded job %s missing from jobs list: %v", jid, jobs)
	}
}

func TestJobStdinRoundtrip(t *testing.T) {
	srv, _ := newTestServer(t)
	c := dialWs(t, srv)
	setStateRevHttp(t, srv, "rev-i")

	// cat blocks on stdin, gets backgrounded, then we feed it.
	r := execResult(t, c, 1, "rev-i", "cat")
	jid, _ := r["job_id"].(string)
	if jid == "" {
		t.Fatalf("cat must be backgrounded: %v", r)
	}

	w := rpcCall(t, c, 2, "job_stdin", map[string]any{"job_id": jid, "data": "hello-stdin\n", "close": true})
	wres := w["result"].(map[string]any)
	if n, _ := wres["written"].(float64); int(n) != len("hello-stdin\n") {
		t.Fatalf("job_stdin written = %v", wres)
	}

	wait := rpcCall(t, c, 3, "job_wait", map[string]any{"job_id": jid, "timeout_ms": 5000})
	if wait["result"].(map[string]any)["state"] != "done" {
		t.Fatalf("cat should finish after EOF: %v", wait["result"])
	}

	out := rpcCall(t, c, 4, "job_output", map[string]any{"job_id": jid})
	lines := out["result"].(map[string]any)["lines"].([]any)
	if len(lines) != 1 || !strings.Contains(lines[0].(string), "hello-stdin") {
		t.Fatalf("stdin roundtrip output = %v", lines)
	}
}

func TestJobOutputGrepPagination(t *testing.T) {
	srv, _ := newTestServer(t)
	c := dialWs(t, srv)
	setStateRevHttp(t, srv, "rev-g")

	// seq writes 30 lines after a short sleep -> backgrounded job.
	r := execResult(t, c, 1, "rev-g", "sleep 0.5 && seq 1 30")
	jid, _ := r["job_id"].(string)
	waitForJobCompleted(t, c)

	// substring filter, case-insensitive by default
	out := rpcCall(t, c, 2, "job_output", map[string]any{"job_id": jid, "grep": "1"})
	lines := out["result"].(map[string]any)["lines"].([]any)
	if len(lines) != 12 { // 1, 10-19, 21
		t.Fatalf("grep '1' lines = %v (%d)", lines, len(lines))
	}

	// regex alternation
	out2 := rpcCall(t, c, 3, "job_output", map[string]any{"job_id": jid, "grep": "^[13]$", "regex": true})
	lines2 := out2["result"].(map[string]any)["lines"].([]any)
	if len(lines2) != 2 {
		t.Fatalf("regex lines = %v", lines2)
	}

	// invert
	out3 := rpcCall(t, c, 4, "job_output", map[string]any{"job_id": jid, "grep": "1", "invert": true})
	lines3 := out3["result"].(map[string]any)["lines"].([]any)
	if len(lines3) != 30-12 {
		t.Fatalf("invert lines = %d, want %d", len(lines3), 30-12)
	}

	// offset/limit pagination
	out4 := rpcCall(t, c, 5, "job_output", map[string]any{"job_id": jid, "offset": 5, "limit": 3})
	res4 := out4["result"].(map[string]any)
	lines4 := res4["lines"].([]any)
	if len(lines4) != 3 || lines4[0].(string) != "6" || lines4[2].(string) != "8" {
		t.Fatalf("offset paging = %v", lines4)
	}
	if res4["total_lines"] != float64(30) {
		t.Fatalf("total = %v", res4["total_lines"])
	}

	// negative offset from the end
	out5 := rpcCall(t, c, 6, "job_output", map[string]any{"job_id": jid, "offset": -2})
	lines5 := out5["result"].(map[string]any)["lines"].([]any)
	if len(lines5) != 2 || lines5[1].(string) != "30" {
		t.Fatalf("negative offset = %v", lines5)
	}
}

func TestEventTailTruncation(t *testing.T) {
	srv, _ := newTestServer(t)
	c := dialWs(t, srv)
	setStateRevHttp(t, srv, "rev-t")

	// ~57KB of output completes fast but exceeds the 16KB sync cap, so it
	// is promoted and the completed event must carry only the tail. The
	// event can even precede the execute response (inline finalize).
	var r map[string]any
	var params map[string]any
	_ = c.WriteMessage(websocket.TextMessage, mustJSON(t, map[string]any{"id": 1, "method": "execute", "params": map[string]any{"command": "seq 1 10000", "rev": "rev-t"}}))
	for r == nil || params == nil {
		m := readUntil(t, c, 5*time.Second, func(m map[string]any) bool {
			if m["event"] == "job.completed" {
				if p, ok := m["params"].(map[string]any); ok {
					params = p
				}
			}
			if v, ok := m["id"]; ok && v == float64(1) {
				if res, ok := m["result"].(map[string]any); ok {
					r = res
				}
			}
			return r != nil && params != nil
		})
		_ = m
	}
	jid, _ := r["job_id"].(string)
	if jid == "" || r["backgrounded"] != true {
		t.Fatalf("large output must be promoted, got %v", r)
	}
	stdout, _ := params["stdout"].(string)
	if len(stdout) > eventOutputTail+1024 {
		t.Fatalf("event stdout len = %d, want <= ~16KB", len(stdout))
	}
	if params["truncated"] != true {
		t.Fatalf("truncated flag = %v, want true", params["truncated"])
	}
	if !strings.Contains(stdout, "10000") {
		t.Fatalf("tail must contain the final lines, tail ends: %q", stdout[len(stdout)-40:])
	}

	// full output still available via job_output
	out := rpcCall(t, c, 2, "job_output", map[string]any{"job_id": jid, "offset": 0, "limit": 1})
	res := out["result"].(map[string]any)
	if res["total_lines"] != float64(10000) {
		t.Fatalf("full output total_lines = %v", res["total_lines"])
	}
}

func TestMergedStreams(t *testing.T) {
	srv, _ := newTestServer(t)
	c := dialWs(t, srv)
	setStateRevHttp(t, srv, "rev-m")

	// stdout and stderr both land in the merged job output.
	o, ec := execWait(t, c, 1, "rev-m", "echo to-out && ls /definitely-missing-file")
	if !strings.Contains(o, "to-out") || !strings.Contains(o, "No such file") {
		t.Fatalf("merged output missing streams: %q", o)
	}
	if ec == 0 {
		t.Fatalf("exit_code should be nonzero after ls failure, got %d", ec)
	}
}
