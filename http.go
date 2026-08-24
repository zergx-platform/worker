package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

func slogJSON(level int, msg string, fields ...any) {
	_ = level
	rec := map[string]any{"msg": msg}
	for i := 0; i+1 < len(fields); i += 2 {
		rec[fmt.Sprint(fields[i])] = fields[i+1]
	}
	b, _ := json.Marshal(rec)
	log.Println(string(b))
}

func buildHandler(state *State) http.Handler {
	store := state.store
	jobs := state.jobs
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"healthy": true})
	})
	mux.HandleFunc("/api/v1/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			writeJSON(w, 200, map[string]any{"jobs": jobsResponse(store)})
			return
		}
		if r.Method == http.MethodDelete {
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			jid, _ := body["job_id"].(string)
			writeJSON(w, 200, map[string]any{"ok": jobs.KillJob(jid)})
			return
		}
		writeJSON(w, 405, map[string]any{"error": "method not allowed"})
	})
	mux.HandleFunc("/api/v1/sync/check", func(w http.ResponseWriter, r *http.Request) {
		handleSyncCheck(w, r, state)
	})
	mux.HandleFunc("/api/v1/sync/files", func(w http.ResponseWriter, r *http.Request) {
		handleSyncFiles(w, r, state)
	})
	mux.HandleFunc("/api/v1/file", func(w http.ResponseWriter, r *http.Request) {
		handleFile(w, r, state.workdir)
	})

	ws := newWSHub(state)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ws":
			ws.serve(w, r)
			return
		case "/ws/job":
			ws.serveJobStream(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func main() {
	if err := os.MkdirAll(workdir, 0o755); err != nil {
		slogJSON(0, "mkdir failed", "err", err)
	}
	if err := os.Chdir(workdir); err != nil {
		slogJSON(0, "chdir failed", "err", err)
	}

	store := NewStore()
	jobs := NewManager(store, workdir)
	state := &State{store: store, jobs: jobs, workdir: workdir, syncedRev: ""}

	srv := &http.Server{
		Addr:    "0.0.0.0:" + port,
		Handler: buildHandler(state),
	}
	slogJSON(0, "recoder-worker listening", "port", port)
	if err := srv.ListenAndServe(); err != nil {
		slogJSON(0, "server error", "err", err)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func jobsResponse(store *Store) []map[string]any {
	out := make([]map[string]any, 0)
	for _, j := range store.ListJobs() {
		out = append(out, map[string]any{
			"id": j.ID, "command": j.Command, "state": j.State,
			"exit_code": j.ExitCode, "started_at": j.StartedAt,
			"finished_at": j.FinishedAt,
		})
	}
	return out
}

type State struct {
	store     *Store
	jobs      *Manager
	workdir   string
	syncedRev string
}

func handleSyncCheck(w http.ResponseWriter, r *http.Request, st *State) {
	root := st.workdir
	rev := r.URL.Query().Get("rev")
	var manifest []struct {
		Path string `json:"path"`
		Size int64  `json:"size"`
	}
	if err := json.NewDecoder(r.Body).Decode(&manifest); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	manifestSet := map[string]bool{}
	for _, e := range manifest {
		manifestSet[e.Path] = true
	}
	missing := []string{}
	for _, e := range manifest {
		fi, err := os.Stat(filepath.Join(root, e.Path))
		if err != nil || fi.Size() != e.Size {
			missing = append(missing, e.Path)
		}
	}
	extra := []string{}
	for _, rel := range listWorkspaceFiles(root) {
		if manifestSet[rel] {
			continue
		}
		extra = append(extra, rel)
		_ = os.Remove(filepath.Join(root, rel))
	}
	if rev != "" {
		st.syncedRev = rev
	}
	writeJSON(w, 200, map[string]any{"missing": missing, "extra": extra})
}

func handleSyncFiles(w http.ResponseWriter, r *http.Request, st *State) {
	root := st.workdir
	rev := r.URL.Query().Get("rev")
	n, err := extractTarball(r.Body, root)
	if err != nil {
		writeJSON(w, 200, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if rev != "" {
		st.syncedRev = rev
	}
	writeJSON(w, 200, map[string]any{"ok": true, "files": n})
}

func handleFile(w http.ResponseWriter, r *http.Request, root string) {
	path := r.URL.Query().Get("path")
	full := filepath.Join(root, path)
	rel, err := filepath.Rel(root, full)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		http.NotFound(w, r)
		return
	}
	if _, err := os.Stat(full); err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("content-type", "application/gzip")
	_ = createTarball(w, root, []string{path})
}
