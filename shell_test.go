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

func fastSync(t *testing.T) {
	t.Helper()
	t.Setenv("WORKER_SYNC_WAIT_MS", "200")
}

func execResult(t *testing.T, c *websocket.Conn, id int, rev, cmd string) map[string]any {
	t.Helper()
	return rpcCall(t, c, id, "execute", map[string]any{"command": cmd, "rev": rev})["result"].(map[string]any)
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

	r1 := execResult(t, c, 1, "rev-a", "echo one && echo two")
	if o := r1["output"].(string); !strings.Contains(o, "one") || !strings.Contains(o, "two") {
		t.Fatalf("&& chain output = %q", o)
	}

	r2 := execResult(t, c, 2, "rev-a", "false || echo fallback")
	if o := r2["output"].(string); !strings.Contains(o, "fallback") {
		t.Fatalf("|| fallback output = %q", o)
	}

	r3 := execResult(t, c, 3, "rev-a", "echo 'a|b>c&d'")
	if o := r3["output"].(string); !strings.Contains(o, "a|b>c&d") {
		t.Fatalf("quoted metachars must stay literal, got %q", o)
	}

	writeTestFile(t, "m.txt", "alpha\nbeta\ngamma\n")
	r4 := execResult(t, c, 4, "rev-a", "grep -E 'lph|mm' m.txt")
	if o := r4["output"].(string); !strings.Contains(o, "alpha") || !strings.Contains(o, "gamma") || strings.Contains(o, "beta") {
		t.Fatalf("quoted regex pipe must pass through, got %q", o)
	}

	r5 := execResult(t, c, 5, "rev-a", "echo x\necho y")
	if o := r5["output"].(string); !strings.Contains(o, "x") || !strings.Contains(o, "y") {
		t.Fatalf("newline as ; output = %q", o)
	}

	// third-party commands stay allowed (no command blacklist).
	r6 := execResult(t, c, 6, "rev-a", "kill -l")
	if ec := int(r6["exit_code"].(float64)); ec != 0 {
		t.Fatalf("kill -l exit = %d, want 0", ec)
	}

	// variable-expansion-time rejections surface as exit 2 + guidance.
	for _, cmd := range []string{"echo $?", "echo $$"} {
		rr := execResult(t, c, 7, "rev-a", cmd)
		if ec := int(rr["exit_code"].(float64)); ec != 2 {
			t.Fatalf("cmd %q exit = %v, want 2", cmd, rr["exit_code"])
		}
		if o := rr["output"].(string); !strings.Contains(o, "special shell parameters") {
			t.Fatalf("cmd %q output = %q, want guidance", cmd, o)
		}
	}
}

func TestSessionPersistence(t *testing.T) {
	srv, _ := newTestServer(t)
	c := dialWs(t, srv)
	setStateRevHttp(t, srv, "rev-s")

	execResult(t, c, 1, "rev-s", "export GREETING=hello-world")
	r := execResult(t, c, 2, "rev-s", "echo $GREETING")
	if o := r["output"].(string); !strings.Contains(o, "hello-world") {
		t.Fatalf("export must persist across executes, got %q", o)
	}

	execResult(t, c, 3, "rev-s", "unset GREETING")
	r2 := execResult(t, c, 4, "rev-s", "echo [$GREETING]")
	if o := r2["output"].(string); !strings.Contains(o, "[]") {
		t.Fatalf("unset must remove the variable, got %q", o)
	}

	writeTestFile(t, "sub/deep.txt", "x")
	execResult(t, c, 5, "rev-s", "cd sub")
	r3 := execResult(t, c, 6, "rev-s", "cat deep.txt")
	if o := r3["output"].(string); !strings.Contains(o, "x") {
		t.Fatalf("cd must persist across executes, got %q", o)
	}
	if ec := int(r3["exit_code"].(float64)); ec != 0 {
		t.Fatalf("cat exit = %d", ec)
	}
	execResult(t, c, 7, "rev-s", "cd ..")
}

func TestBackgroundedPromotion(t *testing.T) {
	srv, _ := newTestServer(t)
	c := dialWs(t, srv)
	setStateRevHttp(t, srv, "rev-b")
	fastSync(t)

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
	fastSync(t)

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
	fastSync(t)

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
	fastSync(t)

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

func TestSyncMergedStreams(t *testing.T) {
	srv, _ := newTestServer(t)
	c := dialWs(t, srv)
	setStateRevHttp(t, srv, "rev-m")

	// stdout and stderr both land in the merged sync output.
	r := execResult(t, c, 1, "rev-m", "echo to-out && ls /definitely-missing-file")
	if o := r["output"].(string); !strings.Contains(o, "to-out") || !strings.Contains(o, "No such file") {
		t.Fatalf("merged output missing streams: %q", o)
	}
	if ec := int(r["exit_code"].(float64)); ec == 0 {
		t.Fatalf("exit_code should be nonzero after ls failure, got %d", ec)
	}
	_ = time.Second
}
