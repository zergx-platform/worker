package main

import (
	"encoding/json"
	"sync"
)

type wsHub struct {
	mu      sync.Mutex
	state   *State
	clients map[*wsClient]bool // /ws RPC clients (job.completed broadcast)

	// SSE per-job output subscribers (the terminal stream is now SSE, not WS).
	sseMu   sync.Mutex
	sseSubs map[string]map[chan sseEvent]struct{}
}

// sseEvent is a single event pushed to an SSE subscriber.
type sseEvent struct {
	Event  string `json:"event"`
	Params any    `json:"params"`
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

// sseBroadcast pushes an event to the SSE subscribers of one job.
func (h *wsHub) sseBroadcast(jobID string, event string, params any) {
	h.sseMu.Lock()
	subs := make([]chan sseEvent, 0)
	for c := range h.sseSubs[jobID] {
		subs = append(subs, c)
	}
	h.sseMu.Unlock()
	for _, c := range subs {
		select {
		case c <- sseEvent{Event: event, Params: params}:
		default:
		}
	}
}

func (h *wsHub) sseSubscribe(jobID string) (chan sseEvent, func()) {
	c := make(chan sseEvent, 256)
	h.sseMu.Lock()
	if h.sseSubs[jobID] == nil {
		h.sseSubs[jobID] = map[chan sseEvent]struct{}{}
	}
	h.sseSubs[jobID][c] = struct{}{}
	h.sseMu.Unlock()
	return c, func() {
		h.sseMu.Lock()
		delete(h.sseSubs[jobID], c)
		h.sseMu.Unlock()
	}
}
