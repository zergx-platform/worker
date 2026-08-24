package main

import (
	"encoding/json"
	"sync"
)

type wsHub struct {
	mu      sync.Mutex
	state   *State
	clients map[*wsClient]bool
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
