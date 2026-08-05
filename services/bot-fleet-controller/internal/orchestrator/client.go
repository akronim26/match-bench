// Package orchestrator implements client behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type State string

const (
	StateCreating    State = "creating"
	StateReady       State = "ready"
	StateFailed      State = "failed"
	StateTerminating State = "terminating"
)

// Endpoint groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Endpoint struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

// Slot groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Slot struct {
	SlotID   string   `json:"slot_id"`
	State    State    `json:"state"`
	Message  string   `json:"message"`
	Endpoint Endpoint `json:"endpoint"`
}

var ErrSlotNotFound = errors.New("slot not found")

// Client groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Client struct {
	baseURL string
	http    *http.Client
}

// defaultSlotHTTPTimeout bounds a single orchestrator call. 180s, not the
// original 15s (raised 2026-08-03 after the first EKS run): slot creation
// blocks until the contestant pod is observable, and on a real registry that
// includes the FIRST PULL of that submission's image. Locally every image was
// pre-imported into containerd, so 15s always sufficed and the limit was
// invisible; in a contest EVERY new submission is a cold pull, and a timeout
// here fails the session with "context deadline exceeded" while the
// orchestrator is still healthily waiting (its own request context is
// canceled mid-flight, which is what the confusing "context canceled" pod-get
// error was). Override with ORCHESTRATOR_HTTP_TIMEOUT (Go duration).
const defaultSlotHTTPTimeout = 180 * time.Second

// NewClient performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewClient(baseURL string) *Client {
	timeout := defaultSlotHTTPTimeout
	if v := os.Getenv("ORCHESTRATOR_HTTP_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			timeout = d
		}
	}
	return &Client{
		baseURL: baseURL,
		http:    &http.Client{Timeout: timeout},
	}
}

// createSlotRequest groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type createSlotRequest struct {
	SlotID       string `json:"slot_id"`
	ContestantID string `json:"contestant_id"`
	Image        string `json:"image"`
	// Ports, not a single Port. A ProtocolAll submission serves FIX on 9898 and
	// REST+WS on 8080, and sandbox-orchestrator has always accepted a `ports` array
	// (createSlotRequest.resolvePorts) — this side simply never sent one. The pod
	// therefore exposed only the submission's primary port, so every REST and WS task
	// connected to a port the pod did not have and timed out. It reproduced with a
	// single task per protocol, which is what ruled out load and concurrency.
	Ports []int `json:"ports"`
	// OrderBand is the session's leased exclusive orders.sent/orders.acked
	// partition band (see bot-fleet-controller's band lease allocator).
	// sandbox-orchestrator forwards it as the ORDER_BAND env var on the
	// capture container so ebpf-latency can partition orders.acked without
	// re-deriving the band by hash.
	OrderBand uint32 `json:"order_band"`
}

// CreateSlot applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *Client) CreateSlot(ctx context.Context, slotID, contestantID, image string, ports []int, orderBand uint32) (*Slot, error) {
	body, err := json.Marshal(createSlotRequest{SlotID: slotID, ContestantID: contestantID, Image: image, Ports: ports, OrderBand: orderBand})
	if err != nil {
		return nil, fmt.Errorf("marshal create slot: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/slots", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	return c.doSlot(req)
}

// GetSlot applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *Client) GetSlot(ctx context.Context, slotID string) (*Slot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/slots/"+slotID, nil)
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	return c.doSlot(req)
}

// DeleteSlot applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *Client) DeleteSlot(ctx context.Context, slotID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+"/slots/"+slotID, nil)
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("delete slot: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || (resp.StatusCode >= 200 && resp.StatusCode < 300) {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	return fmt.Errorf("delete slot: %s — %s", resp.Status, string(body))
}

// WaitForReady applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *Client) WaitForReady(ctx context.Context, slotID string, deadline time.Duration, pollInterval time.Duration) (*Slot, error) {
	if pollInterval == 0 {
		pollInterval = 500 * time.Millisecond
	}
	deadlineCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		slot, err := c.GetSlot(deadlineCtx, slotID)
		if err != nil {
			return nil, err
		}
		switch slot.State {
		case StateReady, StateFailed:
			return slot, nil
		}
		select {
		case <-deadlineCtx.Done():
			return slot, fmt.Errorf("slot did not become ready within %s (last state: %s)", deadline, slot.State)
		case <-ticker.C:
		}
	}
}

// doSlot applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *Client) doSlot(req *http.Request) (*Slot, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("orchestrator request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrSlotNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("orchestrator returned %s — %s", resp.Status, string(body))
	}

	var slot Slot
	if err := json.NewDecoder(resp.Body).Decode(&slot); err != nil {
		return nil, fmt.Errorf("decode slot response: %w", err)
	}
	return &slot, nil
}
