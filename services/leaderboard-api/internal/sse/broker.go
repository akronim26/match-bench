// Package sse implements broker behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package sse

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/schemas/topics"
)

const heartbeatInterval = 15 * time.Second

type SnapshotFunc func(context.Context) (any, error)

type sseMessage struct {
	event string
	data  []byte
}

// Broker groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Broker struct {
	mu        sync.Mutex
	clients   map[chan sseMessage]struct{}
	snapshot  SnapshotFunc
	heartbeat time.Duration
}

// New performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func New(snapshot SnapshotFunc) *Broker {
	return &Broker{clients: make(map[chan sseMessage]struct{}), snapshot: snapshot, heartbeat: heartbeatInterval}
}

// Broadcast applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (b *Broker) Broadcast(ev topics.LeaderboardUpdateEvent) {
	payload, err := json.Marshal(ev)
	if err != nil {
		return
	}
	b.send(sseMessage{event: "update", data: payload})
}

// BroadcastLive marshals payload to JSON and sends it to all connected clients as a "live_metrics" event.
func (b *Broker) BroadcastLive(payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	b.send(sseMessage{event: "live_metrics", data: data})
}

func (b *Broker) send(msg sseMessage) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.clients {
		select {
		case ch <- msg:
		default:
			close(ch)
			delete(b.clients, ch)
			metrics.Counter("leaderboard_api_sse_dropped_clients_total", "Leaderboard SSE clients dropped because they could not keep up.", nil, 1)
		}
	}
	metrics.Gauge("leaderboard_api_sse_clients", "Connected leaderboard SSE clients.", nil, float64(len(b.clients)))
}

// ServeHTTP applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := make(chan sseMessage, 16)
	b.mu.Lock()
	b.clients[ch] = struct{}{}
	metrics.Gauge("leaderboard_api_sse_clients", "Connected leaderboard SSE clients.", nil, float64(len(b.clients)))
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		if _, ok := b.clients[ch]; ok {
			delete(b.clients, ch)
			close(ch)
		}
		metrics.Gauge("leaderboard_api_sse_clients", "Connected leaderboard SSE clients.", nil, float64(len(b.clients)))
		b.mu.Unlock()
	}()

	if b.snapshot != nil {
		snap, err := b.snapshot(r.Context())
		if err != nil {
			http.Error(w, "snapshot failed", http.StatusServiceUnavailable)
			return
		}
		writeEvent(w, "snapshot", snap)
		flusher.Flush()
	}
	keepalive := time.NewTicker(b.heartbeat)
	defer keepalive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepalive.C:
			if _, err := w.Write([]byte(": keepalive\n\n")); err != nil {
				return
			}
			flusher.Flush()
		case msg, ok := <-ch:
			if !ok {
				return
			}
			w.Write([]byte("event: " + msg.event + "\n"))
			w.Write([]byte("data: "))
			w.Write(msg.data)
			w.Write([]byte("\n\n"))
			flusher.Flush()
		}
	}
}

// writeEvent performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func writeEvent(w http.ResponseWriter, name string, v any) {
	payload, _ := json.Marshal(v)
	w.Write([]byte("event: " + name + "\n"))
	w.Write([]byte("data: "))
	w.Write(payload)
	w.Write([]byte("\n\n"))
}

// ClientCount applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (b *Broker) ClientCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.clients)
}
