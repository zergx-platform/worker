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
	h := &wsHub{state: st, clients: map[*wsClient]bool{}}
	st.jobs.SetCompletionHandler(func(ev JobCompletion) {
		b, _ := json.Marshal(ev)
		params := json.RawMessage(b)
		h.broadcastEvent("job.completed", params)
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
