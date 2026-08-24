package main

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

type wsClient struct {
	mu   sync.Mutex
	conn *websocket.Conn
}

func (c *wsClient) sendJSON(v any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.WriteJSON(v)
}

func newWSHub(st *State) *wsHub {
	h := &wsHub{
		state:   st,
		clients: map[*wsClient]bool{},
		streams: map[string]map[*wsClient]bool{},
	}
	st.jobs.SetCompletionHandler(func(ev JobCompletion) {
		b, _ := json.Marshal(ev)
		h.broadcastEvent("job.completed", json.RawMessage(b))
		// Per-job terminal subscribers also learn the job finished.
		h.streamEvent(ev.JobID, "job.completed", ev)
	})
	st.jobs.SetOutputHandler(func(jobID, stream, content string) {
		h.streamEvent(jobID, "job.output", map[string]string{
			"job_id":  jobID,
			"stream":  stream,
			"content": content,
		})
	})
	return h
}

func (h *wsHub) serve(w http.ResponseWriter, r *http.Request) {
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	conn, err := up.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c := &wsClient{conn: conn}

	h.mu.Lock()
	h.clients[c] = true
	h.mu.Unlock()

	defer func() {
		h.mu.Lock()
		delete(h.clients, c)
		for jid := range h.streams {
			delete(h.streams[jid], c)
		}
		h.mu.Unlock()
		conn.Close()
	}()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		go h.handleMessage(c, msg)
	}
}

// serveJobStream is a dedicated terminal endpoint: it replays the job's
// history (last 100 rows) then streams live job.output events for that job
// only.
func (h *wsHub) serveJobStream(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Query().Get("job_id")
	if jobID == "" {
		http.Error(w, "job_id required", http.StatusBadRequest)
		return
	}
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	conn, err := up.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c := &wsClient{conn: conn}

	h.mu.Lock()
	if h.streams[jobID] == nil {
		h.streams[jobID] = map[*wsClient]bool{}
	}
	h.streams[jobID][c] = true
	h.mu.Unlock()

	defer func() {
		h.mu.Lock()
		if subs := h.streams[jobID]; subs != nil {
			delete(subs, c)
		}
		h.mu.Unlock()
		conn.Close()
	}()

	// Replay history: last 100 rows, then mark the end of replay.
	rows := h.state.store.Rows(jobID)
	replay := rows
	if len(replay) > 100 {
		replay = replay[len(replay)-100:]
	}
	for _, row := range replay {
		c.sendJSON(map[string]any{
			"event":  "job.output",
			"params": map[string]string{"job_id": jobID, "stream": row.Stream, "content": row.Content},
		})
	}
	c.sendJSON(map[string]any{"event": "job.history_end", "params": map[string]int{"total": len(rows), "replayed": len(replay)}})

	// Drain the socket until the client disconnects (keepalive read loop).
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

func (h *wsHub) dispatch(method string, params map[string]json.RawMessage) (any, string) {
	switch method {
	case "execute":
		return h.cmdExecute(params)
	case "file_read":
		return h.cmdFileRead(params)
	case "file_write":
		return h.cmdFileWrite(params)
	case "file_delete":
		return h.cmdFileDelete(params)
	case "jobs":
		return h.cmdJobs(params)
	case "kill":
		return h.cmdKill(params)
	case "job_output":
		return h.cmdJobOutput(params)
	case "job_wait":
		return h.cmdJobWait(params)
	case "job_stdin":
		return h.cmdJobStdin(params)
	default:
		return nil, "unknown method: " + method
	}
}

type rpcRequest struct {
	ID     any                        `json:"id"`
	Method string                     `json:"method"`
	Params map[string]json.RawMessage `json:"params"`
}

func (h *wsHub) handleMessage(c *wsClient, raw []byte) {
	var req rpcRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		c.sendJSON(map[string]any{"error": "invalid JSON"})
		return
	}
	res, resErr := h.dispatch(req.Method, req.Params)
	if resErr != "" {
		c.sendJSON(map[string]any{"id": req.ID, "error": resErr})
		return
	}
	c.sendJSON(map[string]any{"id": req.ID, "result": res})
}
