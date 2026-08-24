package main

import (
	"encoding/json"
	"sync"
)

type wsHub struct {
	mu      sync.Mutex
	state   *State
	clients map[*wsClient]bool            // /ws RPC clients (job.completed broadcast)
	streams map[string]map[*wsClient]bool // jobID -> terminal subscribers (job.output streaming)
}

func (h *wsHub) broadcastEvent(event string, params json.RawMessage) {
	payload, _ := json.Marshal(map[string]any{"event": event, "params": params})
	h.mu.Lock()
	clients := make([]*wsClient, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.Unlock()
	for _, c := range clients {
		c.mu.Lock()
		_ = c.conn.WriteMessage(1, payload) // 1 = TextMessage
		c.mu.Unlock()
	}
}

// streamEvent pushes an event only to the subscribers of one job (the
// per-job terminal stream).
func (h *wsHub) streamEvent(jobID string, event string, params any) {
	payload, _ := json.Marshal(map[string]any{"event": event, "params": params})
	h.mu.Lock()
	subs := make([]*wsClient, 0)
	for c := range h.streams[jobID] {
		subs = append(subs, c)
	}
	h.mu.Unlock()
	for _, c := range subs {
		c.mu.Lock()
		_ = c.conn.WriteMessage(1, payload)
		c.mu.Unlock()
	}
}
